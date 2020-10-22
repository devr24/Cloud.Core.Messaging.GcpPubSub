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
        private bool _disposedValue;

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
                return _managerClient ??= new SubscriberServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
            }
        }

        internal PublisherServiceApiClient PublisherClient
        {
            get
            {
                return _publisherClient ??= new PublisherServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
            }
        }

        /// <summary>
        /// Gets or sets the name for the implementor of the INamedInstance interface.
        /// </summary>
        /// <value>The name.</value>
        public string Name { get; set; }

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
        /// The messaging entity manager.
        /// </summary>
        /// <value>The entity manager.</value>
        /// <seealso cref="T:Cloud.Core.IMessageEntityManager" />
        public IMessageEntityManager EntityManager
        {
            get
            {
                return _pubSubManager ??= new PubSubManager(Config.ProjectId, ManagementClient, PublisherClient);
            }
        }

        /// <summary>
        /// Sends the message (with properties) to the specified topic name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="message">The message.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task Send<T>(string topicName, T message, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await InternalSendBatch(topicName, new List<T> { message }, properties, null, 1);
        }

        /// <summary>
        /// Sends the list of messages (with properties) to the specified topic name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task Send<T>(string topicName, IEnumerable<T> messages, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await InternalSendBatch(topicName, messages, properties, null, 1);
        }

        /// <summary>
        /// Send a typed message as PubSub message content to the sender topic.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="message">The message body that we are sending.</param>
        /// <param name="properties">Any additional message properties to add.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper</returns>
        public async Task Send<T>(T message, KeyValuePair<string, object>[] properties = null) where T : class
        {
            await SendBatch(new List<T> { message }, p => properties, 1);
        }

        /// <summary>
        /// Send a batch of typed messages, as PubSub messages, to the sender topic.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="messages">IEnumerable of messages to send.</param>
        /// <param name="batchSize">Size of the message batch to send.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, int batchSize = 10) where T : class
        {
            await SendBatch(messages, p => null, batchSize);
        }

        /// <summary>
        /// Send a batch of typed messages with properties, as PubSub messages, to the sender topic.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="messages">The messages to send.</param>
        /// <param name="properties">The properties to associate with ALL messages.</param>
        /// <param name="batchSize">Size of each batch.</param>
        /// <returns>Task.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, KeyValuePair<string, object>[] properties, int batchSize = 100) where T : class
        {
            await SendBatch(messages, p => properties, batchSize);
        }

        /// <summary>
        /// Sends the batch with a function to set the properties based on the message.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are sending.</typeparam>
        /// <param name="messages">The messages to send.</param>
        /// <param name="setProps">The function to set props for each message.</param>
        /// <param name="batchSize">Size of each batch.</param>
        /// <returns>Task.</returns>
        public async Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) where T : class
        {
            if (Config.Sender == null)
                throw new InvalidOperationException("Sender must be configured to send messages");

            await InternalSendBatch(Config.Sender.TopicRelativeName, messages, null, setProps, batchSize);
        }

        /// <summary>
        /// Gets a single message of type T.
        /// </summary>
        /// <typeparam name="T">The type of the message returned.</typeparam>
        /// <returns>The typed T.</returns>
        public T ReceiveOne<T>() where T : class
        {
            var result = InternalReceiveBatch<T>(1).GetAwaiter().GetResult();
            return result?.FirstOrDefault()?.Body;
        }

        /// <summary>
        /// Gets a single message with IMessageEntity wrapper.
        /// </summary>
        /// <typeparam name="T">Type of message entity body.</typeparam>
        /// <returns>IMessageEntity wrapper with body and properties.</returns>
        public IMessageEntity<T> ReceiveOneEntity<T>() where T : class
        {
            var result = InternalReceiveBatch<T>(1).GetAwaiter().GetResult();
            return result.FirstOrDefault();
        }

        /// <summary>
        /// Read a batch of typed messages in a synchronous manner.
        /// </summary>
        /// <typeparam name="T">Type of object on the entity.</typeparam>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>IMessageItem&lt;T&gt;.</returns>
        public async Task<List<T>> ReceiveBatch<T>(int batchSize) where T : class
        {
            var results = await ReceiveBatchEntity<T>(batchSize);
            return results.Select(b => b.Body).ToList();
        }

        /// <summary>
        /// Receives a batch of message in a synchronous manner of type IMessageEntity types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            return await InternalReceiveBatch<T>(batchSize);
        }

        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T" />.
        /// If you try to setup more then one callback to the same message type <typeparamref name="T" /> you'll get an <see cref="T:System.InvalidOperationException" />.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to.</typeparam>
        /// <param name="successCallback">The <see cref="T:System.Action`1" /> delegate that will be called for each message received.</param>
        /// <param name="errorCallback">The <see cref="T:System.Action`1" /> delegate that will be called when an error occurs.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue.</param>
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
                        var typedContent = GetTypedMessageContent<T>(message);

                        successCallback(typedContent);

                        return Task.FromResult(SubscriberClient.Reply.Ack);
                    }
                    catch (Exception e)
                    {
                        _logger?.LogError(e, "An error occured retrieving messages");
                        errorCallback(e);
                        return Task.FromResult(SubscriberClient.Reply.Nack);
                    }
                }

                return Task.FromResult(SubscriberClient.Reply.Nack);
            });
        }

        /// <summary>
        /// Set up the required receive pipeline, for the given message type, and return a reactive <see cref="T:System.IObservable`1" /> that you can subscribe to.
        /// </summary>
        /// <typeparam name="T">The type of the message returned by the observable.</typeparam>
        /// <param name="batchSize">The size of the batch when reading from a queue.</param>
        /// <returns>The typed <see cref="T:System.IObservable`1" /> that you subscribed to.</returns>
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

        /// <summary>
        /// Stop receiving a message type.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are cancelling the receive on.</typeparam>
        public void CancelReceive<T>() where T : class
        {
            _receiveCancellationToken.Cancel();
            _receiverClient?.StopAsync(_receiveCancellationToken.Token).GetAwaiter().GetResult();
            _receiverClient = null;
        }

        /// <summary>
        /// Update the receiver details
        /// </summary>
        /// <param name="entityName">The name of the entity to listen to.</param>
        /// <param name="entitySubscriptionName">The name of the subscription on the entity to listen to.</param>
        /// <param name="entityFilter">A filter that will be applied to the entity if created through this method.</param>
        /// <returns>Task.</returns>
        public async Task UpdateReceiver(string entityName, string entitySubscriptionName = null, KeyValuePair<string, string>? entityFilter = null)
        {
            Config.ReceiverConfig.EntityName = entityName;
            Config.ReceiverConfig.EntitySubscriptionName = entitySubscriptionName;

            _receiveCancellationToken.Cancel();
            if (_receiverClient != null)
                await _receiverClient.StopAsync(_receiveCancellationToken.Token);
            _receiverClient = null;
        }

        /// <summary>
        /// Reads the properties.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message.</param>
        /// <returns>System.Collections.Generic.IDictionary&lt;System.String, System.Object&gt;.</returns>
        public IDictionary<string, object> ReadProperties<T>(T message) where T : class
        {
            T msg = !(message is IMessageEntity<T> entityMessage) ? message : entityMessage.Body;

            return ReadProperties(Messages[msg]?.Message);
        }

        /// <summary>
        /// Completes the message and removes from the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message we want to complete.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper</returns>
        public async Task Complete<T>(T message) where T : class
        {
            await CompleteAll(new[] { message });
        }

        /// <summary>
        /// Completes all.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="messages">The messages.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
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

        /// <summary>
        /// Abandons a message by returning it to the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message we want to abandon.</param>
        /// <param name="propertiesToModify">The message properties to modify on abandon.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper.</returns>
        public Task Abandon<T>(T message, KeyValuePair<string, object>[] propertiesToModify = null) where T : class
        {
            T msg = !(message is IMessageEntity<T> entityMessage) ? message : entityMessage.Body;
            return Task.FromResult(Messages.TryRemove(msg, out _));
        }

        /// <summary>
        /// Errors a message by moving it specifically to the error queue (dead-letter).
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message that we want to move to the error queue.</param>
        /// <param name="reason">(optional) The reason for erroring the message.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper</returns>
        public async Task Error<T>(T message, string reason = null) where T : class
        {
            T msg = !(message is IMessageEntity<T> entityMessage) ? message : entityMessage.Body;

            var props = ReadProperties(message);
            props.TryAdd("ErrorReason", reason);

            // Complete the message, then send on to dead-letter queue.
            await CompleteAll(new[] { msg });
            await InternalSendBatch(Config.ReceiverConfig.TopicDeadletterRelativeName, new List<T> { msg }, props.ToArray(), null, 100);
        }

        /// <summary>
        /// Defers a message in the the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message we want to abandon.</param>
        /// <param name="propertiesToModify">The message properties to modify on abandon.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper.</returns>
        public Task Defer<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Receives a batch of deferred messages of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="identities">The list of identities pertaining to the batch.</param>
        /// <returns>IMessageItem&lt;T&gt;.</returns>
        public Task<List<T>> ReceiveDeferredBatch<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Receives a batch of deferred messages of type IMessageEntity types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="identities">The list of identities pertaining to the batch</param>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        public Task<List<IMessageEntity<T>>> ReceiveDeferredBatchEntity<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        #region IDisposable Support

        /// <summary>
        /// Disposes the specified disposing.
        /// </summary>
        /// <param name="disposing">The disposing flag.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    // Any clear up here.
                }

                _disposedValue = true;
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        private void CreateIfNotExists()
        {
            // Build receiverConfig.
            if (!_createdReceiverTopic && Config.ReceiverConfig != null && Config.ReceiverConfig.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.ReceiverConfig.EntityName, Config.ReceiverConfig.DeadLetterEntityName,
                    Config.ReceiverConfig.EntitySubscriptionName, Config.ReceiverConfig.EntityFilter?.Value).GetAwaiter().GetResult();
                _createdReceiverTopic = true;
            }

            // Build sender.
            if (!_createdSenderTopic && Config.Sender != null && Config.Sender.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.Sender.EntityName, Config.Sender.DeadLetterEntityName).GetAwaiter().GetResult();
                _createdSenderTopic = true;
            }
        }

        [ExcludeFromCodeCoverage]
        private ChannelCredentials GetCredentials()
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

        private async Task InternalSendBatch<T>(string topic, IEnumerable<T> messages, KeyValuePair<string, object>[] properties, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize) where T : class
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

        private async Task<List<IMessageEntity<T>>> InternalReceiveBatch<T>(int batchSize) where T : class
        {
            // Ensure the receiver topic/subscription is setup first of all before trying to receive.
            CreateIfNotExists();

            var batch = new List<IMessageEntity<T>>();

            // Determine whether to read from deal-letter topic or normal topic, depending on the flag.
            var topicName = Config.ReceiverConfig.ReadFromErrorEntity
                ? Config.ReceiverConfig.DeadLetterEntityName
                : Config.ReceiverConfig.EntitySubscriptionName;

            // Make the read request to Gcp PubSub.
            PullResponse response = await ManagementClient.PullAsync(new SubscriptionName(Config.ProjectId, topicName), false, batchSize);
            var messages = response.ReceivedMessages;

            if (!messages.Any())
                return batch;

            // Loop through each message and get the typed equivalent.
            foreach (var message in messages)
            {
                var typedContent = GetTypedMessageContent<T>(message.Message);

                // Keep track of the read messages so they can be completed.
                Messages.TryAdd(typedContent, message);

                var props = ReadProperties(message.Message);

                batch.Add(new PubSubMessageEntity<T> { Body = typedContent, Properties = props });
            }

            return batch;
        }

        /// <summary>
        /// Extracts and deserializes a given message
        /// </summary>
        /// <param name="message">The message that needs deserialized</param>
        /// <returns>T.</returns>
        /// <exception cref="InvalidOperationException">Cannot access the message content for message {message.MessageId}</exception>
        private T GetTypedMessageContent<T>(PubsubMessage message) where T : class
        {
            string content = message.Data.ToStringUtf8();

            // Check for no content (we cannot process this).
            if (content.IsNullOrEmpty())
                throw new InvalidOperationException($"Cannot access the message content for message {message.MessageId}");

            try
            {
                // Deserialize to a specific type.
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

                // NOTE: Maybe should automatically dead-letter the message if conversion fails?
                return null;
            }
        }

        /// <summary>
        /// Reads the list of properties from the PubSub message.
        /// </summary>
        /// <param name="pubsubMsg">The PubSub message.</param>
        /// <returns>System.Collections.Generic.IDictionary&lt;System.String, System.Object&gt;.</returns>
        private IDictionary<string, object> ReadProperties(PubsubMessage pubsubMsg)
        {
            var props = new Dictionary<string, object>();

            // Add each property to the list of props returned.
            foreach (var messageAttribute in pubsubMsg.Attributes)
            {
                props.Add(messageAttribute.Key, messageAttribute.Value);
            }
            // Always add MessageId as a property.
            props.AddOrUpdate("MessageId", pubsubMsg.MessageId);
            return props;
        }

    }
}
