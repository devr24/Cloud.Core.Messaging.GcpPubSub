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
                // Lazy creation of the subscriber client when requested.
                return _managerClient ??= new SubscriberServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
            }
        }

        internal PublisherServiceApiClient PublisherClient
        {
            get
            {
                // Lazy creation of the publisher client when requested.
                return _publisherClient ??= new PublisherServiceApiClientBuilder { ChannelCredentials = GetCredentials() }.Build();
            }
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
                // Lazy creation of the PubSub manager when requested for the first time.
                return _pubSubManager ??= new PubSubManager(Config.ProjectId, ManagementClient, PublisherClient);
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
            // Validate configuration and throw if invalid.
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
            // Validate configuration and throw if invalid.
            config.ThrowIfInvalid();

            Config = config;
            _logger = logger;
            _jsonAuthFile = config.JsonAuthFile;

            Name = config.ProjectId;
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
            // Calls internal send method directly, so topic name can be passed along.
            await InternalSendBatch(new TopicName(Config.ProjectId, topicName).ToString(), new List<T> { message }, properties, null, 1);
        }

        /// <summary>
        /// Sends the list of messages (with properties) to the specified topic name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="messages">The messages.</param>
        /// <param name="properties">The properties.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task SendBatch<T>(string topicName, IEnumerable<T> messages, KeyValuePair<string, object>[] properties = null) where T : class
        {
            // Calls internal send method directly, so topic name can be passed along.
            await InternalSendBatch(new TopicName(Config.ProjectId, topicName).ToString(), messages, properties, null, 1);
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
        /// <typeparam name="T"></typeparam>
        /// <param name="subscriptionName">The subscription to receive from.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task<T> ReceiveOne<T>(string subscriptionName) where T : class
        {
            var result = await InternalReceiveBatch<T>(subscriptionName, 1);
            return result?.FirstOrDefault()?.Body;
        }

        /// <summary>
        /// Read a batch of messages from the given subscription name name.
        /// </summary>
        /// <typeparam name="T">Type of object returned.</typeparam>
        /// <param name="subscriptionName">The subscription to receive from.</param>
        /// <param name="batchSize">Size of the batch of messages to retrieve.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
        public async Task<IEnumerable<IMessageEntity<T>>> ReceiveBatch<T>(string subscriptionName, int batchSize = 100) where T : class
        {
            return await InternalReceiveBatch<T>(subscriptionName, batchSize);
        }

        /// <summary>
        /// Gets a single message of type T.
        /// </summary>
        /// <typeparam name="T">The type of the message returned.</typeparam>
        /// <returns>The typed T.</returns>
        public T ReceiveOne<T>() where T : class
        {
            // Batch size is one, so list of one should be returned - first item body will be returned from the method.
            var result = ReceiveBatch<T>(1).GetAwaiter().GetResult();
            return result.FirstOrDefault();
        }

        /// <summary>
        /// Gets a single message with IMessageEntity wrapper.
        /// </summary>
        /// <typeparam name="T">Type of message entity body.</typeparam>
        /// <returns>IMessageEntity wrapper with body and properties.</returns>
        public IMessageEntity<T> ReceiveOneEntity<T>() where T : class
        {
            // Batch size is one, so list of one should be returned - first item body will be returned from the method.
            var result = ReceiveBatchEntity<T>(1).GetAwaiter().GetResult();
            return result.FirstOrDefault();
        }

        /// <summary>
        /// Read a batch of typed messages.
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
        /// Receives a batch of messages of type IMessageEntity.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        /// <exception cref="InvalidOperationException">ReceiverConfig must be set to read messages.</exception>
        public async Task<List<IMessageEntity<T>>> ReceiveBatchEntity<T>(int batchSize) where T : class
        {
            // Ensure config is setup.
            if (Config.ReceiverConfig == null)
                throw new InvalidOperationException("Receiver configuration must be set");

            // Determine whether to read from deal-letter topic or normal topic, depending on the flag.
            var subscriptionName = Config.ReceiverConfig.ReadFromErrorEntity
                ? Config.ReceiverConfig.EntityDeadLetterSubscriptionName
                : Config.ReceiverConfig.EntitySubscriptionName;

            return await InternalReceiveBatch<T>(subscriptionName, batchSize);
        }

        /// <summary>
        /// Sets up a call back for receiving any message of type <typeparamref name="T" />.
        /// If you try to setup more then one callback to the same message type <typeparamref name="T" /> you'll get an <see cref="T:System.InvalidOperationException" />.
        /// </summary>
        /// <typeparam name="T">The type of the message that we are subscribing to.</typeparam>
        /// <param name="successCallback">The <see cref="T:System.Action`1" /> delegate that will be called for each message received.</param>
        /// <param name="errorCallback">The <see cref="T:System.Action`1" /> delegate that will be called when an error occurs.</param>
        /// <param name="batchSize">The size of the batch when reading for a queue.</param>
        /// <exception cref="InvalidOperationException">ReceiverConfig must be set to read messages.</exception>
        public void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 10) where T : class
        {
            // Ensure config is setup.
            if (Config.ReceiverConfig == null)
                throw new InvalidOperationException("Receiver configuration must be set");

            CreateIfNotExists();

            _receiverClient ??= SubscriberClient.CreateAsync(new SubscriptionName(Config.ProjectId, Config.ReceiverConfig.ReadFromErrorEntity
                    ? Config.ReceiverConfig.EntityDeadLetterName
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
        /// <exception cref="InvalidOperationException">ReceiverConfig must be set to read messages.</exception>
        public IObservable<T> StartReceive<T>(int batchSize = 10) where T : class
        {
            // Ensure config is setup.
            if (Config.ReceiverConfig == null)
                throw new InvalidOperationException("Receiver configuration must be set");

            CreateIfNotExists();

            _receiverClient ??= SubscriberClient.CreateAsync(new SubscriptionName(Config.ProjectId, Config.ReceiverConfig.ReadFromErrorEntity
                    ? Config.ReceiverConfig.EntityDeadLetterName
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
            _receiverClient?.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            _receiverClient = null;
        }

        /// <summary>
        /// Update the receiver details
        /// </summary>
        /// <param name="entityName">The name of the entity to listen to.</param>
        /// <param name="entitySubscriptionName">The name of the subscription on the entity to listen to.</param>
        /// <param name="entityFilter">A filter that will be applied to the entity if created through this method.</param>
        /// <returns>Task.</returns>
        public Task UpdateReceiver(string entityName, string entitySubscriptionName = null, KeyValuePair<string, string>? entityFilter = null)
        {
            Config.ReceiverConfig.EntitySubscriptionName = entitySubscriptionName;
            Config.ReceiverConfig.EntityName = entityName;

            _receiverClient?.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            _receiverClient = null;

            return Task.FromResult(true);
        }

        /// <summary>
        /// Reads the properties.
        /// Returns null if message was not found, otherwise returns a dictionary of properties.
        /// </summary>
        /// <typeparam name="T">Type of message to lookup.</typeparam>
        /// <param name="message">The message to read properties from.</param>
        /// <returns>System.Collections.Generic.IDictionary&lt;System.String, System.Object&gt; Dictionary of properties found.</returns>
        public IDictionary<string, object> ReadProperties<T>(T message) where T : class
        {
            // Null safe guard.
            if (message == null)
                return null;


            //// This conversion picks up IMessageEntity messages, because their body is used for the key in the message dictionary.
            //if (message != null && !Messages.TryRemove(message, out _))
            //{
            //    if (TryGetMessageEntityBody(message, out var body))
            //    {
            //        return Task.FromResult(Messages.TryRemove(body, out _));
            //    }
            //}
            var isEntity = TryGetMessageEntityBody(message, out var msgBody);
            var msg = !isEntity ? message : msgBody;
            if (!Messages.TryGetValue(msg, out var pubSubMessage))
            {
                // Cant find message, return null.
                return null;
            }

            return InternalReadProperties(pubSubMessage.Message);
        }

        /// <summary>
        /// Completes the message and removes from the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message we want to complete.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper</returns>
        public async Task Complete<T>(T message) where T : class
        {
            // Null safe guard.
            if (message == null)
                return;

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
                // Safe-guard against nulls.
                if (message == null) return; 

                // Find the message, remove from dictionary and grab acknowledge id if existed.
                if (Messages.TryRemove(message, out var foundMsg))
                {
                    ackIds.Add(foundMsg.AckId);
                }
                else
                {
                    // As the message wasn't found using the message as key, this conversion try's to picks up messages of type
                    // IMessageEntity because in their case it's their body that is used for the key in the dictionary.
                    var isEntity = TryGetMessageEntityBody(message, out var msgBody);

                    if (isEntity && Messages.TryRemove(msgBody, out foundMsg))
                    {
                        ackIds.Add(foundMsg.AckId);
                    }
                    
                }
            }

            if (ackIds.Any())
            {
                // Acknowledge message and remove from subscription.
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
            // This conversion picks up IMessageEntity messages, because their body is used for the key in the message dictionary.
            if (message!= null && !Messages.TryRemove(message, out _))
            {
                if (TryGetMessageEntityBody(message, out var body))
                {
                    return Task.FromResult(Messages.TryRemove(body, out _));
                }
            }

            return Task.FromResult(false);

            // Note: We don't complete the message here so it can be picked back up again. Just remove local reference.
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
            // Null safe guard.
            if (message == null)
                return;

            object msgBody = message;

            if (!Messages.TryGetValue(message, out var foundMsg))
            {
                // As the message wasn't found using the message as key, this conversion try's to picks up messages of type
                // IMessageEntity because in their case it's their body that is used for the key in the dictionary.
                var isEntity = TryGetMessageEntityBody(message, out var body);
                if (isEntity && Messages.TryGetValue(body, out foundMsg))
                {
                    msgBody = body;
                }
                else
                {
                    // NOT FOUND!
                    return;
                }
            }

            var props = InternalReadProperties(foundMsg.Message);

            // Add an error reason property if set.
            if (!reason.IsNullOrEmpty())
            {
                props.TryAdd("ErrorReason", reason);
            }
           
            // Complete the message (completing removes the message from the dictionary), then send on to dead-letter queue.
            await CompleteAll(new[] { msgBody });
            await InternalSendBatch(Config.ReceiverConfig.TopicDeadletterRelativeName, new [] { msgBody }, props.ToArray(), null, 100);
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Defers a message in the the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message">The message we want to abandon.</param>
        /// <param name="propertiesToModify">The message properties to modify on abandon.</param>
        /// <returns>The async <see cref="T:System.Threading.Tasks.Task" /> wrapper.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task Defer<T>(T message, KeyValuePair<string, object>[] propertiesToModify) where T : class
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Receives a batch of deferred messages of type T.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="identities">The list of identities pertaining to the batch.</param>
        /// <returns>IMessageItem&lt;T&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<List<T>> ReceiveDeferredBatch<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Receives a batch of deferred messages of type IMessageEntity types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="identities">The list of identities pertaining to the batch</param>
        /// <returns>IMessageEntity&lt;T&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<List<IMessageEntity<T>>> ReceiveDeferredBatchEntity<T>(IEnumerable<long> identities) where T : class
        {
            throw new NotImplementedException();
        }

        /// <summary>Creates topics and subscriptions if they do not exist.</summary>
        private void CreateIfNotExists()
        {
            // Build receiverConfig.
            if (!_createdReceiverTopic && Config.ReceiverConfig != null && Config.ReceiverConfig.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.ReceiverConfig.EntityName, Config.ReceiverConfig.EntityDeadLetterName,
                    Config.ReceiverConfig.EntitySubscriptionName, Config.ReceiverConfig.EntityDeadLetterSubscriptionName, Config.ReceiverConfig.EntityFilter?.Value).GetAwaiter().GetResult();
                _createdReceiverTopic = true;
            }

            // Build sender.
            if (!_createdSenderTopic && Config.Sender != null && Config.Sender.CreateEntityIfNotExists)
            {
                ((PubSubManager)EntityManager).CreateTopic(Config.Sender.EntityName, Config.Sender.EntityDeadLetterName).GetAwaiter().GetResult();
                _createdSenderTopic = true;
            }
        }

        /// <summary>Gets the google API credentials.</summary>
        /// <returns>Grpc.Core.ChannelCredentials.</returns>
        [ExcludeFromCodeCoverage]
        private ChannelCredentials GetCredentials()
        {
            // Only build these once.
            if (_credentials == null)
            {
                // NOTE: extend here to add other auth mechanisms.
                if (!_jsonAuthFile.IsNullOrEmpty())
                {
                    // Authenticate using a reference to json files.
                    _credentials = GoogleCredential.FromFile(_jsonAuthFile).ToChannelCredentials();
                }
                else
                {
                    // Default method, which takes the environment variable "GOOGLE_APPLICATION_CREDENTIALS" with the credential file path.

                    // Firstly, verify the credentials have been set as expected - throw error if not.
                    var credentialLocation = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
                    if (credentialLocation.IsNullOrEmpty() || File.Exists(credentialLocation) == false)
                    {
                        throw new InvalidOperationException("Environment variable \"GOOGLE_APPLICATION_CREDENTIALS\" must exist and must point to a valid credential json file");
                    }

                    // Use default credentials.
                    _credentials = GoogleCredential.GetApplicationDefault().ToChannelCredentials();
                }
            }

            return _credentials;
        }

        /// <summary>
        /// Works out if the type {T} message is IMessageEntity type, and if so, returns the MessageEntity.Body.
        /// Used when looking up the key in the message dictionary.
        /// </summary>
        /// <typeparam name="T">Type of the message.</typeparam>
        /// <param name="message">The message.</param>
        /// <param name="entity">The entity output if found to be MessageEntity. Taken from Body property.</param>
        /// <returns>System.Boolean.</returns>
        private bool TryGetMessageEntityBody<T>(T message, out object entity) where T : class
        {
            const string messageEntityName = "IMessageEntity`1";
            entity = null;

            var typeName = typeof(T).Name;

            // Check if the type of the message is the same as the message entity type.
            if (typeName == messageEntityName)
            {
                // Grab the object from the MessageEntity.Body property.
                var body = message.GetPropertyValueByName("body");
                entity = body;
                return true; // True, it was message entity type.
            }

            return false; // False, it was not message entity type.
        }

        /// <summary>
        /// Internals the send batch.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic">The topic to send to.</param>
        /// <param name="messages">The messages to send.</param>
        /// <param name="properties">The properties for each message (if all will be the same).</param>
        /// <param name="setProps">The function for setting props for each method (allows messages to have individual props).</param>
        /// <param name="batchSize">Size of the batch to send.</param>
        /// <returns>System.Threading.Tasks.Task.</returns>
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

        /// <summary>
        /// Internals the receive batch.  Always returns list of messages back but will be empty if none where returned from the server.
        /// </summary>
        /// <typeparam name="T">Type of messages to serialize to.</typeparam>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="batchSize">Size of the batch.</param>
        /// <returns>System.Threading.Tasks.Task&lt;System.Collections.Generic.List&lt;Cloud.Core.IMessageEntity&lt;T&gt;&gt;&gt;.</returns>
        private async Task<List<IMessageEntity<T>>> InternalReceiveBatch<T>(string subscriptionName, int batchSize) where T : class
        {
            // Ensure the receiver topic/subscription is setup first of all before trying to receive.
            CreateIfNotExists();

            var batch = new List<IMessageEntity<T>>();

            // Make the read request to Gcp PubSub.
            PullResponse response = await ManagementClient.PullAsync(new SubscriptionName(Config.ProjectId, subscriptionName), false, batchSize);
            var messages = response.ReceivedMessages;

            // Return empty batch if no messages where returned.
            if (!messages.Any())
                return batch;

            // Loop through each message and get the typed equivalent.
            foreach (var message in messages)
            {
                var typedContent = GetTypedMessageContent<T>(message.Message);

                // Keep track of the read messages so they can be completed.
                Messages.TryAdd(typedContent, message);

                var props = InternalReadProperties(message.Message);

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
        private IDictionary<string, object> InternalReadProperties(PubsubMessage pubsubMsg)
        {
            var props = new Dictionary<string, object>();

            // Add each property to the list of props returned.
            if (pubsubMsg != null)
            {
                foreach (var messageAttribute in pubsubMsg.Attributes)
                {
                    props.Add(messageAttribute.Key, messageAttribute.Value);
                }

                // Always add MessageId as a property.
                props.AddOrUpdate("MessageId", pubsubMsg.MessageId);
            }

            return props;
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
    }
}
