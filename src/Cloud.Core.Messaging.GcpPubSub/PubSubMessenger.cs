using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Cloud.Core.Comparer;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Cloud.Core.Messaging.GcpPubSub
{
    public class PubSubMessenger : IMessenger, IReactiveMessenger
    {
        private readonly PubSubConfig _config;
        private PublisherServiceApiClient _publisherClient;
        private SubscriberClient _receiverClient;
        private SubscriberServiceApiClient _receiverSubscriptionClient;
        private CancellationTokenSource _receiveCancellationToken;
        private readonly ILogger _logger;
        internal readonly ConcurrentDictionary<object, ReceivedMessage> Messages = new ConcurrentDictionary<object, ReceivedMessage>(ObjectReferenceEqualityComparer<object>.Default);
        internal readonly ISubject<object> MessagesIn = new Subject<object>();

        public PubSubMessenger(PubSubConfig config, ILogger logger = null)
        {
            // Verify the credentials have been set as expected.
            ValidateCredentials();

            _config = config;
            _logger = logger;
        }

        internal SubscriberClient ReceiverClient
        {
            get
            {
                if (_receiverClient == null)
                {

                    _receiveCancellationToken = new CancellationTokenSource();
                    var subscriptionName = new SubscriptionName(_config.ProjectId, _config.SubscriptionName);
                    _receiverClient = SubscriberClient.CreateAsync(subscriptionName).GetAwaiter().GetResult(); TopicName topicName = new TopicName(_config.ProjectId, _config.TopicId);
                    _receiverSubscriptionClient = SubscriberServiceApiClient.Create();
                    _receiverSubscriptionClient.CreateSubscription(new Subscription());
                }
                return _receiverClient;
            }
        }

        internal PublisherServiceApiClient PublisherClient
        {
            get
            {
                if (_publisherClient == null)
                    _publisherClient = PublisherServiceApiClient.Create();

                return _publisherClient;
            }
        }

        public string Name { get; set; }

        public IMessageEntityManager EntityManager { get; }

        public async Task Send<T>(T message, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await SendBatch(_config.TopicRelativeName, new List<T> { message }, null, null, 1);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, int batchSize = 10) where T : class
        {
            await SendBatch(_config.TopicRelativeName, messages, null, null, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, KeyValuePair<string, object>[] properties, int batchSize = 100) where T : class
        {
            await SendBatch(_config.TopicRelativeName, messages, properties, null, batchSize);
        }

        public async Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) where T : class
        {
            await SendBatch(_config.TopicRelativeName, messages, null, setProps, batchSize);
        }

        internal async Task SendBatch<T>(string topic, IEnumerable<T> messages, KeyValuePair<string, object>[] properties, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize) where T : class
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

        public T ReceiveOne<T>() where T : class
        {
            return ReceiveOneEntity<T>()?.Body;
        }

        public IMessageEntity<T> ReceiveOneEntity<T>() where T : class
        {
            var result = ReceiveBatchEntity<T>(1).GetAwaiter().GetResult();
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
            var results = await ReceiveBatchEntity<T>(batchSize);
            return results.Select(b => b.Body).ToList();
        }

        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            var batch = new List<PubSubEntity<T>>();

            try
            {
                PullResponse response = await _receiverSubscriptionClient.PullAsync(_config.SubscriptionName, true, batchSize);

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
                _logger.LogError(e, "Error during read of pub/sub message");
                return null;
            }

            return batch as List<IMessageEntity<T>>;
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
            _config.TopicId = entityName;
            _config.SubscriptionName = entitySubscriptionName;
            _config.DeadLetterTopicId = entityDeadLetterName;

            return Task.FromResult(true);
        }

        public IDictionary<string, object> ReadProperties<T>(T msg) where T : class
        {
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
                if (Messages.TryGetValue(message, out var msg))
                {
                    ackIds.Add(msg.AckId);
                }
            }
            await _receiverSubscriptionClient.AcknowledgeAsync(new AcknowledgeRequest { AckIds = { ackIds }, Subscription = _config.SubscriptionName });
        }

        public Task Abandon<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            return Task.FromResult(Messages.TryRemove(message, out var msg));
        }

        public async Task Error<T>(T message, string reason = null) where T : class
        {
            var props = ReadProperties(message);
            props.TryAdd("ErrorReason", reason);

            // Complete the message, then send on to dead-letter queue.
            await CompleteAll(new[] { message });
            await SendBatch(_config.DeadLetterTopicRelativeName, new List<T> { message }, props.ToArray(), null, 100);
        }

        public string GetSignedAccessUrl(ISignedAccessConfig accessConfig)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }

        internal void ValidateCredentials()
        {
            var credentialLocation = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
            if (credentialLocation.IsNullOrEmpty() || File.Exists(credentialLocation) == false)
            {
                throw new InvalidOperationException("Environment variable \"GOOGLE_APPLICATION_CREDENTIALS\" must exist and must point to a valid credential json file");
            }
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
    }
}
