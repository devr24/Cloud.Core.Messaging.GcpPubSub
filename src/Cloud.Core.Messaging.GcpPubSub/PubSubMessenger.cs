using Google.Apis.Auth.OAuth2;
using Grpc.Auth;

namespace Cloud.Core.Messaging.GcpPubSub
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Reactive;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Threading;
    using System.Threading.Tasks;
    using Exceptions;
    using Comparer;
    using Google.Cloud.PubSub.V1;
    using Google.Protobuf;
    using Grpc.Core;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    /// <summary>
    /// Class for implementing GCP PubSub Messenger.
    /// Implements the <see cref="IMessenger" />
    /// Implements the <see cref="IReactiveMessenger" />
    /// </summary>
    /// <seealso cref="IMessenger" />
    /// <seealso cref="IReactiveMessenger" />
    public class PubSubMessenger : IMessenger, IReactiveMessenger
    {
        private readonly PubSubConfig _config;
        private PublisherServiceApiClient _publisherClient;
        private string _jsonAuthPath;
        private SubscriberClient _receiverClient;
        private SubscriberServiceApiClient _managerClient;
        private CancellationTokenSource _receiveCancellationToken = new CancellationTokenSource();
        private PubSubManager _pubSubManager;
        private readonly GoogleCredential _credential;
        private bool _createdTopics;
        private bool _initialisedClients;

        private readonly ILogger _logger;

        internal readonly ConcurrentDictionary<object, ReceivedMessage> Messages =
            new ConcurrentDictionary<object, ReceivedMessage>(ObjectReferenceEqualityComparer<object>.Default);

        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        internal SubscriberServiceApiClient ManagementClient
        {
            get
            {
                InitialiseClients();
                return _managerClient;
            }
        }

        internal SubscriberClient ReceiverClient
        {
            get
            {
                InitialiseClients();
                CreateIfNotExists();
                return _receiverClient;
            }
        }

        internal PublisherServiceApiClient PublisherClient
        {
            get
            {
                InitialiseClients();
                CreateIfNotExists();
                return _publisherClient;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PubSubMessenger"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        public PubSubMessenger([NotNull]PubSubConfig config, ILogger logger = null)
        {
            // Verify the credentials have been set as expected.
            ValidateCredentials();

            // Validate configuration.
            config.ThrowIfInvalid();
             
            _config = config;
            _logger = logger;
            _credential = GoogleCredential.GetApplicationDefault();

            Name = config.ProjectId;
        }

        public PubSubMessenger([NotNull] JsonAuthConfig config, ILogger logger = null)
        {
            // Validate configuration.
            config.ThrowIfInvalid();

            _jsonAuthPath = config.JsonAuthFile;
            _config = config;
            _logger = logger;
            _credential = GoogleCredential.FromFile(_jsonAuthPath); 

            Name = config.ProjectId;
        }

        /// <summary>
        /// Gets or sets the name for the implementor of the INamedInstance interface.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

        public IMessageEntityManager EntityManager
        {
            get
            {
                InitialiseClients();
                return _pubSubManager;
            }
        }

        public async Task Send<T>(T message, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await SendBatch(new List<T> { message }, p => null, 1);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, int batchSize = 10) where T : class
        {
            await SendBatch(messages, p => null, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, KeyValuePair<string, object>[] properties, int batchSize = 100) where T : class
        {
            await SendBatch(messages, p=> properties, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) where T : class
        {
            if (_config.Sender == null)
                throw new InvalidOperationException("Sender must be configured to send messages");

            await InternalSendBatch(_config.Sender.TopicRelativeName, messages, null, setProps, batchSize);
        }
        
        public T ReceiveOne<T>() where T : class
        {
            var result = InternalReceiveBatch<T>(1).GetAwaiter().GetResult();
            return result?.FirstOrDefault()?.Body;
        }

        public IMessageEntity<T> ReceiveOneEntity<T>() where T : class
        {
            var result = InternalReceiveBatch<T>(1).GetAwaiter().GetResult();
            return result.FirstOrDefault();
        }

        public void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 10) where T : class
        {
            ReceiverClient.StartAsync((message, cancel) =>
            {
                if (!cancel.IsCancellationRequested)
                {
                    try
                    {
                        var typed = GetTypedMessageContent<T>(message);
                        successCallback(typed);
                    }
                    catch (Exception e)
                    {
                        _logger?.LogError(e, "An error occured retrieving messages");
                        errorCallback(e);
                    }
                }

                return Task.FromResult(SubscriberClient.Reply.Nack);
            });
        }

        public async Task<List<T>> ReceiveBatch<T>(int batchSize) where T : class
        {
            var results = await InternalReceiveBatch<T>(batchSize);
            return results.Select(b => b.Body).ToList();
        }

        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            return await InternalReceiveBatch<T>(batchSize);
        }

        public IObservable<T> StartReceive<T>(int batchSize = 10) where T : class
        {
            IObserver<T> messageIn = MessagesIn.AsObserver();

            ReceiverClient.StartAsync((message, cancel) =>
            {
                if (!cancel.IsCancellationRequested)
                {
                    var typed = GetTypedMessageContent<T>(message);
                    messageIn.OnNext(typed);
                }

                return Task.FromResult(SubscriberClient.Reply.Nack);
            });

            return MessagesIn.OfType<T>();
        }

        public void CancelReceive<T>() where T : class
        {
            _receiveCancellationToken.Cancel();
            ReceiverClient.StopAsync(_receiveCancellationToken.Token);
        }

        public Task UpdateReceiver(string entityName, string entitySubscriptionName = null, bool createIfNotExists = false,
            KeyValuePair<string, string>? entityFilter = null, string entityDeadLetterName = null)
        {
            _config.Receiver.EntityName= entityName;
            _config.Receiver.EntitySubscriptionName = entitySubscriptionName;

            _receiveCancellationToken.Cancel();
            _receiverClient.StopAsync(_receiveCancellationToken.Token);
            _receiverClient = null;
            
            return Task.FromResult(true);
        }

        public IDictionary<string, object> ReadProperties<T>(T message) where T : class
        {
            var entityMessage = message as IMessageEntity<T>;
            T msg = entityMessage == null ? message : entityMessage.Body;

            return ReadProperties(Messages[msg]?.Message);
        }

        public async Task Complete<T>(T message) where T : class
        {
            await CompleteAll(new[] { message });
        }

        public async Task CompleteAll<T>(IEnumerable<T> messages) where T : class
        {
            var ackIds = new List<string>();

            foreach (var message in messages)
            {
                var entityMessage = message as IMessageEntity<T>;
                T msg = entityMessage == null ? message : entityMessage.Body;
                if (msg != null && Messages.TryGetValue(msg, out var foundMsg))
                {
                    ackIds.Add(foundMsg.AckId);
                }
            }

            if (ackIds.Any())
            {
                await ManagementClient.AcknowledgeAsync(new AcknowledgeRequest { AckIds = { ackIds }, Subscription = new SubscriptionName(_config.ProjectId, _config.Receiver.EntitySubscriptionName).ToString() });
            }
        }

        public Task Abandon<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            var entityMessage = message as IMessageEntity<T>;
            T msg = entityMessage == null ? message : entityMessage.Body;
            return Task.FromResult(Messages.TryRemove(msg, out _));
        }

        public async Task Error<T>(T message, string reason = null) where T : class
        {
            var entityMessage = message as IMessageEntity<T>;
            T msg = entityMessage == null ? message : entityMessage.Body;

            var props = ReadProperties(message);
            props.TryAdd("ErrorReason", reason);

            // Complete the message, then send on to dead-letter queue.
            await CompleteAll(new[] { msg });
            await InternalSendBatch(_config.Receiver.DeadLetterTopicRelativeName, new List<T> { msg }, props.ToArray(), null, 100);
        }

        public string GetSignedAccessUrl(ISignedAccessConfig accessConfig)
        {
            throw new NotImplementedException();
        }

        public Task Defer<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            throw new NotImplementedException();
        }

        public Task<List<T>> ReceiveDeferredBatch<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        public Task<List<IMessageEntity<T>>> ReceiveDeferredBatchEntity<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }


        private void InitialiseClients()
        {
            if (_initialisedClients) return;

            // Get the google credentials.
            var channelCredentials = _credential.ToChannelCredentials();

            _managerClient ??= new SubscriberServiceApiClientBuilder { ChannelCredentials = channelCredentials }.Build();
            _publisherClient ??= new PublisherServiceApiClientBuilder { ChannelCredentials = channelCredentials }.Build();
            _pubSubManager ??= new PubSubManager(_config.ProjectId, _managerClient, _publisherClient);
            _receiverClient ??= SubscriberClient.CreateAsync(new SubscriptionName(_config.ProjectId, _config.Receiver.EntitySubscriptionName),
                new SubscriberClient.ClientCreationSettings(credentials: channelCredentials)).GetAwaiter().GetResult();

            _initialisedClients = true;
        }

        private void CreateIfNotExists()
        {
            if (_createdTopics)
                return;

            // Build receiver.
            if (_config.Receiver != null && _config.Receiver.CreateEntityIfNotExists)
            {
                _pubSubManager.CreateTopic(_config.ProjectId, _config.Receiver.EntityName, _config.Receiver.DeadLetterEntityName,
                    _config.Receiver.EntitySubscriptionName, _config.Receiver.EntityFilter?.Value).GetAwaiter().GetResult();
            }

            // Build sender.
            if (_config.Sender != null && _config.Sender.CreateEntityIfNotExists)
            {
                _pubSubManager.CreateTopic(_config.ProjectId, _config.Sender.TopicId, _config.Sender.DeadLetterEntityName).GetAwaiter().GetResult();
            }

            _createdTopics = true;
        }

        internal void ValidateCredentials()
        {
            var credentialLocation = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
            if (credentialLocation.IsNullOrEmpty() || File.Exists(credentialLocation) == false)
            {
                throw new InvalidOperationException("Environment variable \"GOOGLE_APPLICATION_CREDENTIALS\" must exist and must point to a valid credential json file");
            }
        }

        internal async Task InternalSendBatch<T>(string topic, IEnumerable<T> messages, KeyValuePair<string, object>[] properties, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize) where T : class
        {
            var batchMsgCount = 0;
            var isByteArray = typeof(T) == typeof(byte[]);
            var publishRequest = new PublishRequest
            {
                Topic = topic // "projects/{projectId}/topics/{topicName}"
            };

            foreach (var msg in messages)
            {
                var byteStr = isByteArray ? ByteString.CopyFrom(msg as byte[]) : ByteString.CopyFromUtf8(JsonConvert.SerializeObject(msg));
                var sendMsg = new PubsubMessage { Data = byteStr };

                KeyValuePair<string, object>[] props = properties ?? setProps?.Invoke(msg);

                if (props != null)
                {
                    foreach (var (key, value) in setProps(msg))
                        sendMsg.Attributes.Add(key, value.ToString());
                }

                publishRequest.Messages.Add(sendMsg);

                if (batchMsgCount > batchSize)
                {
                    // Publish a message to the topic using PublisherClient.
                    await PublisherClient.PublishAsync(publishRequest);
                    publishRequest.Messages.Clear();
                    batchMsgCount = 0;
                }

                batchMsgCount++;
            }

            // Catch any remaining messages.
            if (publishRequest.Messages.Count > 0)
                await PublisherClient.PublishAsync(publishRequest);
        }

        internal async Task<List<IMessageEntity<T>>> InternalReceiveBatch<T>(int batchSize) where T : class
        {
            var batch = new List<IMessageEntity<T>>();

            try
            {
                PullResponse response = await ManagementClient.PullAsync(new SubscriptionName(_config.ProjectId, _config.Receiver.EntitySubscriptionName), true, batchSize);

                var message = response.ReceivedMessages.FirstOrDefault();

                if (message == null)
                    return null;

                var typedContent = GetTypedMessageContent<T>(message.Message);

                Messages.TryAdd(typedContent, message);

                var props = ReadProperties(message.Message);

                batch.Add(new PubSubEntity<T> { Body = typedContent, Properties = props });
            }
            catch (RpcException e)
            {
                _logger?.LogError(e, "Error during read of pub/sub message");
                return null;
            }

            return batch;
        }

        private IDictionary<string, object> ReadProperties(PubsubMessage pubsubMsg)
        {
            var props = new Dictionary<string, object>();
            foreach (var messageAttribute in pubsubMsg.Attributes)
            {
                props.Add(messageAttribute.Key, messageAttribute.Value);
            }
            props.AddOrUpdate("MessageId", pubsubMsg.MessageId);
            return props;
        }

        /// <summary>
        /// Extracts and deserializes a given message
        /// </summary>
        /// <param name="message">The message that needs deserialized</param>
        /// <returns>T.</returns>
        /// <exception cref="InvalidOperationException">Cannot access the message content for message {message.MessageId}</exception>
        internal T GetTypedMessageContent<T>(PubsubMessage message) where T : class
        {
            string content = message.Data.ToStringUtf8();

            // Check for no content (we cannot process this).
            if (content.IsNullOrEmpty())
                throw new InvalidOperationException($"Cannot access the message content for message {message.MessageId}");

            try
            {
                return JsonConvert.DeserializeObject<T>(content);
            }
            catch (Exception ex) when (ex is JsonReaderException || ex is JsonSerializationException)
            {
                // If we are actually expecting T to be a system type, just return without serialization.
                if (typeof(T).IsSystemType())
                {
                    return content as T;
                }

                _logger?.LogWarning($"Could not map message to {typeof(T)}, sending message to error flow");

                //TODO: Dead letter the message??

                return null;
            }
        }
    }
}
