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
    using Comparer;
    using Extensions;
    using Google.Apis.Auth.OAuth2;
    using Google.Cloud.PubSub.V1;
    using Google.Protobuf;
    using Grpc.Auth;
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
        private readonly ISubject<object> _messagesIn = new Subject<object>();
        private readonly CancellationTokenSource _receiveCancellationToken = new CancellationTokenSource();
        private readonly ILogger _logger;
        private readonly string _jsonAuthFile;

        private PublisherServiceApiClient _publisherClient;
        private SubscriberClient _receiverClient;
        private SubscriberServiceApiClient _managerClient;
        private PubSubManager _pubSubManager;
        private bool _createdReceiverTopic;
        private bool _createdSenderTopic;
        private ChannelCredentials _credentials;

        internal readonly PubSubConfig Config;
        internal readonly ConcurrentDictionary<object, ReceivedMessage> Messages = new ConcurrentDictionary<object, ReceivedMessage>(ObjectReferenceEqualityComparer<object>.Default);

        internal SubscriberServiceApiClient ManagementClient
        {
            get
            {
                _managerClient ??= new SubscriberServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
                return _managerClient;
            }
        }

        internal PublisherServiceApiClient PublisherClient
        {
            get
            {
                _publisherClient ??= new PublisherServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
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
            // Validate configuration.
            config.ThrowIfInvalid();
             
            Config = config;
            _logger = logger;

            Name = config.ProjectId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PubSubMessenger"/> class.
        /// </summary>
        /// <param name="config">The configuration.</param>
        /// <param name="logger">The logger.</param>
        public PubSubMessenger([NotNull] PubSubJsonAuthConfig config, ILogger logger = null)
        {
            // Validate configuration.
            config.ThrowIfInvalid();

            Config = config;
            _logger = logger;
            _jsonAuthFile = config.JsonAuthFile;

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
                _pubSubManager ??= new PubSubManager(Config.ProjectId, ManagementClient, PublisherClient);
                return _pubSubManager;
            }
        }

        public async Task Send<T>(T message, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await SendBatch(new List<T> { message }, p => properties, 1);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, int batchSize = 10) where T : class
        {
            await SendBatch(messages, p => null, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, KeyValuePair<string, object>[] properties, int batchSize = 100) where T : class
        {
            await SendBatch(messages, p => properties, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) where T : class
        {
            if (Config.Sender == null)
                throw new InvalidOperationException("Sender must be configured to send messages");

            await InternalSendBatch(Config.Sender.TopicRelativeName, messages, null, setProps, batchSize);
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

        public async Task<List<T>> ReceiveBatch<T>(int batchSize) where T : class
        {
            var results = await ReceiveBatchEntity<T>(batchSize);
            return results.Select(b => b.Body).ToList();
        }

        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            return await InternalReceiveBatch<T>(batchSize);
        }

        public void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 10) where T : class
        {
            CreateIfNotExists();
            _receiverClient ??= SubscriberClient.CreateAsync(new SubscriptionName(Config.ProjectId, Config.ReceiverConfig.ReadFromErrorEntity
                    ? Config.ReceiverConfig.DeadLetterEntityName
                    : Config.ReceiverConfig.EntitySubscriptionName),
                new SubscriberClient.ClientCreationSettings(credentials: GetCredentials())).GetAwaiter().GetResult();

            _receiverClient.StartAsync((message, cancel) =>
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

        public IObservable<T> StartReceive<T>(int batchSize = 10) where T : class
        {
            CreateIfNotExists();
            _receiverClient ??= SubscriberClient.CreateAsync(new SubscriptionName(Config.ProjectId, Config.ReceiverConfig.ReadFromErrorEntity
                    ? Config.ReceiverConfig.DeadLetterEntityName
                    : Config.ReceiverConfig.EntitySubscriptionName),
                new SubscriberClient.ClientCreationSettings(credentials: GetCredentials())).GetAwaiter().GetResult();

            IObserver<T> messageIn = _messagesIn.AsObserver();

            _receiverClient.StartAsync((message, cancel) =>
            {
                if (!cancel.IsCancellationRequested)
                {
                    var typed = GetTypedMessageContent<T>(message);
                    messageIn.OnNext(typed);
                }

                return Task.FromResult(SubscriberClient.Reply.Nack);
            });

            return _messagesIn.OfType<T>();
        }

        public void CancelReceive<T>() where T : class
        {
            _receiveCancellationToken.Cancel();
            _receiverClient.StopAsync(_receiveCancellationToken.Token);
            _receiverClient = null;
        }

        public Task UpdateReceiver(string entityName, string entitySubscriptionName = null, KeyValuePair<string, string>? entityFilter = null)
        {
            Config.ReceiverConfig.EntityName= entityName;
            Config.ReceiverConfig.EntitySubscriptionName = entitySubscriptionName;

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
                if (Messages.TryRemove(message, out var foundMsg))
                {
                    ackIds.Add(foundMsg.AckId);
                }
                else
                {
                    var body = message.GetPropertyValueByName("body");
                    if (body != null && Messages.TryRemove(body, out foundMsg))
                    {
                        ackIds.Add(foundMsg.AckId);
                    }
                }
            }

            if (ackIds.Any())
            {
                await ManagementClient.AcknowledgeAsync(new AcknowledgeRequest { AckIds = { ackIds }, Subscription = new SubscriptionName(Config.ProjectId, Config.ReceiverConfig.EntitySubscriptionName).ToString() });
            }
        }

        public Task Abandon<T>(T message, KeyValuePair<string, object>[] propertiesToModify = null) where T : class
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
            await InternalSendBatch(Config.ReceiverConfig.TopicDeadletterRelativeName, new List<T> { msg }, props.ToArray(), null, 100);
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

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // Any clear up here.
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion

        internal void CreateIfNotExists()
        {
            // Build receiverConfig.
            if (!_createdReceiverTopic && Config.ReceiverConfig != null && Config.ReceiverConfig.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.ProjectId, Config.ReceiverConfig.EntityName, Config.ReceiverConfig.DeadLetterEntityName,
                    Config.ReceiverConfig.EntitySubscriptionName, Config.ReceiverConfig.EntityFilter?.Value).GetAwaiter().GetResult();
                _createdReceiverTopic = true;
            }

            // Build sender.
            if (!_createdSenderTopic && Config.Sender != null && Config.Sender.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.ProjectId, Config.Sender.EntityName, Config.Sender.DeadLetterEntityName).GetAwaiter().GetResult();
                _createdSenderTopic = true;
            }
        }

        [ExcludeFromCodeCoverage]
        internal ChannelCredentials GetCredentials()
        {
            if (_credentials == null)
            {
                if (!_jsonAuthFile.IsNullOrEmpty())
                {
                    _credentials = GoogleCredential.FromFile(_jsonAuthFile).ToChannelCredentials();
                }
                else
                {
                    // Verify the credentials have been set as expected.
                    var credentialLocation = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
                    if (credentialLocation.IsNullOrEmpty() || File.Exists(credentialLocation) == false)
                    {
                        throw new InvalidOperationException(
                            "Environment variable \"GOOGLE_APPLICATION_CREDENTIALS\" must exist and must point to a valid credential json file");
                    }

                    _credentials = GoogleCredential.GetApplicationDefault().ToChannelCredentials();
                }
            }

            return _credentials;
        }

        internal async Task InternalSendBatch<T>(string topic, IEnumerable<T> messages, KeyValuePair<string, object>[] properties, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize) where T : class
        {
            CreateIfNotExists();

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
                    foreach (var (key, value) in props)
                        sendMsg.Attributes.Add(key, value.ToString());
                }

                publishRequest.Messages.Add(sendMsg);

                if (publishRequest.Messages.Count >= batchSize)
                {
                    // Publish a message to the topic using PublisherClient.
                    await PublisherClient.PublishAsync(publishRequest);
                    publishRequest.Messages.Clear();
                }
            }

            // Catch any remaining messages.
            if (publishRequest.Messages.Count > 0)
            {
                await PublisherClient.PublishAsync(publishRequest);
            }
        }

        internal async Task<List<IMessageEntity<T>>> InternalReceiveBatch<T>(int batchSize) where T : class
        {
            CreateIfNotExists();
            var batch = new List<IMessageEntity<T>>();

            var topicName = Config.ReceiverConfig.ReadFromErrorEntity
                ? Config.ReceiverConfig.DeadLetterEntityName
                : Config.ReceiverConfig.EntitySubscriptionName;

            PullResponse response = await ManagementClient.PullAsync(new SubscriptionName(Config.ProjectId, topicName), false, batchSize);
            var messages = response.ReceivedMessages;

            if (!messages.Any())
                return batch;

            foreach (var message in messages)
            {
                var typedContent = GetTypedMessageContent<T>(message.Message);

                Messages.TryAdd(typedContent, message);

                var props = ReadProperties(message.Message);

                batch.Add(new PubSubEntity<T> { Body = typedContent, Properties = props });
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
