namespace Cloud.Core.Messaging.GcpPubSub
{
    using System;
    using System.Threading.Tasks;
    using Google.Api.Gax.ResourceNames;
    using Google.Cloud.PubSub.V1;
    using Grpc.Core;

    /// <summary>
    /// Class PubSub Manager.
    /// Implements the <see cref="IMessageEntityManager" />
    /// </summary>
    /// <seealso cref="IMessageEntityManager" />
    public class PubSubManager : IMessageEntityManager
    {
        private readonly PublisherServiceApiClient _publisherServiceApiClient;
        private readonly SubscriberServiceApiClient _receiverSubscriptionClient;
        private readonly string _projectId;

        /// <summary>
        /// Initializes a new instance of the <see cref="PubSubManager"/> class.
        /// </summary>
        /// <param name="projectId">The project identifier.</param>
        /// <param name="receiverSubscriptionClient">The receiver subscription client.</param>
        /// <param name="publisherServiceApiClient">The publisher service API client.</param>
        internal PubSubManager(string projectId, SubscriberServiceApiClient receiverSubscriptionClient, PublisherServiceApiClient publisherServiceApiClient)
        {
            _projectId = projectId;
            _receiverSubscriptionClient = receiverSubscriptionClient;
            _publisherServiceApiClient = publisherServiceApiClient;
        }

        /// <summary>
        /// Creates the specified topic, subscription and filter.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="deadletterName">Name of the dead-letter.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="filter">The filter for the subscription.</param>
        public async Task CreateTopic(string topicName, string deadletterName, string subscriptionName = null, string filter = null)
        {
            // Create the topic and the dead-letter equivalent.
            CreateTopicIfNotExists(topicName).GetAwaiter().GetResult();
            CreateTopicIfNotExists(deadletterName).GetAwaiter().GetResult();

            if (subscriptionName.IsNullOrEmpty())
                return;

            // If a subscription has been requested for creation, create. Along with dead-letter subscription.
            await CreateSubscription(topicName, subscriptionName, deadletterName, filter);
            await CreateSubscription(deadletterName, deadletterName, null, null);
        }

        /// <summary>
        /// Creates the specified subscription.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="deadletterTopic">The deadletter topic.</param>
        /// <param name="filter">The filter.</param>
        public async Task CreateSubscription(string topicName, string subscriptionName, string deadletterTopic, string filter)
        {
            var createSubscription = new Subscription {
                SubscriptionName = new SubscriptionName(_projectId, subscriptionName),
                Topic = new TopicName(_projectId, topicName).ToString(),
                Filter = filter
            };

            try
            {
                if (!deadletterTopic.IsNullOrEmpty())
                {
                    // Create dead-letter topic subscription.
                    createSubscription.DeadLetterPolicy = new DeadLetterPolicy
                    {
                        MaxDeliveryAttempts = 12,
                        DeadLetterTopic = new TopicName(_projectId, deadletterTopic).ToString()
                    };
                }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {

            }

            try
            {
                // Create topic subscription.
                await _receiverSubscriptionClient.CreateSubscriptionAsync(createSubscription);
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {

            }
        }

        /// <summary>Deletes the specified subscription.</summary>
        /// <param name="subscriptionName">Name of the subscription.</param>
        public async Task DeleteSubscription(string subscriptionName)
        {
            // Delete the specified subscription.
            await _receiverSubscriptionClient.DeleteSubscriptionAsync(new DeleteSubscriptionRequest {
                Subscription = subscriptionName
            });
        }

        /// <summary>
        /// Check if the subscription exists.
        /// </summary>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns><c>true</c> if exists, <c>false</c> otherwise.</returns>
        public async Task<bool> SubscriptionExists(string subscriptionName)
        {
            try
            {
                await _receiverSubscriptionClient.GetSubscriptionAsync(new GetSubscriptionRequest
                {
                    SubscriptionAsSubscriptionName = new SubscriptionName(_projectId, subscriptionName),
                });

                // Found subscription, return true.
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                // Not found error, return false.
                return false;
            }
        }

        /// <summary>Check if the topic exists.</summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns><c>true</c> if exists, <c>false</c> otherwise.</returns>
        public async Task<bool> TopicExists(string topicName)
        {
            try
            {
                await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest
                {
                    TopicAsTopicName = new TopicName(_projectId, topicName)
                });

                // Found subscription, return true.
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                // Not found error, return false.
                return false;
            }
        }

        /// <summary>Deletes the specified topic.</summary>
        /// <param name="topicName">Name of the topic.</param>
        public async Task DeleteTopic(string topicName)
        {
            try
            {
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(_projectId, topicName));
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                // If it wasn't found, then do nothing as non-existence was the desired result.
            }
        }

        /// <summary>Creates the topic if it does not exist.</summary>
        /// <param name="topicName">Name of the topic.</param>
        public async Task CreateTopicIfNotExists(string topicName)
        {
            try
            {
                await _publisherServiceApiClient.CreateTopicAsync(new TopicName(_projectId, topicName));
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
                // If it already exists do nothing as that was the desired outcome.
            }
        }

        #region Interface Methods

        /// <summary>Creates the PubSub entity.</summary>
        /// <param name="entity">The entity.</param>
        public async Task CreateEntity(IMessageEntityConfig entity)
        {
            if (entity is PubSubEntityConfig psConfig)
            {
                string subName = null;
                string filter = null;

                // Cast to config that has subscriptions to see if they are set.
                if (psConfig is ReceiverConfig receiver)
                {
                    subName = receiver.EntitySubscriptionName;
                    filter = receiver.EntityFilter.GetValueOrDefault().Value;
                }

                // Create topic plus subscriptions if set.
                await CreateTopic(psConfig.EntityName, psConfig.DeadLetterEntityName, subName, filter);
            }
        }

        /// <summary>Deletes the entity.</summary>
        /// <param name="entityName">Name of the entity to delete.</param>
        /// <returns>Task.</returns>
        public async Task DeleteEntity(string entityName)
        {
            try
            {
                // Enumerate each subscription and delete
                var subs = _receiverSubscriptionClient.ListSubscriptions(new ListSubscriptionsRequest
                {
                    ProjectAsProjectName = new ProjectName(_projectId)
                });

                // Note: this is inefficient as it parses ALL subscriptions to find the ones relevant to the topic. Be better if
                // there was a way to ask just for the subscriptions for a specific topic.
                
                // Begin by deleting the topic.
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(_projectId, entityName));

                foreach (var subscription in subs)
                {
                    // Delete subscriptions associated with the topic.
                    if (subscription.Topic == entityName)
                        await DeleteSubscription(subscription.Name);
                }
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {

            }
        }

        /// <summary>Check if the entity exists.</summary>
        /// <param name="entityName">Name of the entity to check exists.</param>
        /// <returns>Task.</returns>
        public async Task<bool> EntityExists(string entityName)
        {
            return await TopicExists(entityName);
        }

        /// <summary>
        /// Gets the receiver entity usage percentage. 1.0 represents 100%.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<decimal> GetReceiverEntityUsagePercentage()
        {
            throw new NotImplementedException();
            //var topic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            //var size = topic.CalculateSize();
            //return size;
        }

        /// <summary>
        /// Gets the receiver active and errored message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<EntityMessageCount> GetReceiverMessageCount()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the sender entity usage percentage. 1.0 represents 100%.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<decimal> GetSenderEntityUsagePercentage()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the sender active and errored message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<EntityMessageCount> GetSenderMessageCount()
        {
            throw new NotImplementedException();
            //var activeTopic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            //var deadLetterTopic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            //return new EntityMessageCount
            //{
            //    ActiveEntityCount = activeTopic.CalculateSize(),
            //    ErroredEntityCount = deadLetterTopic.CalculateSize()
            //};
        }

        /// <summary>
        /// Determines whether [receiver entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<bool> IsReceiverEntityDisabled()
        {
            throw new NotImplementedException();
            //var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            //var sub = _receiverSubscriptionClient.GetSubscription(new SubscriptionName("", ""));

            //return (subscriptions != null && subscriptions.ToList().Count > 0);
        }

        /// <summary>
        /// Determines whether [sender entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task<bool> IsSenderEntityDisabled()
        {
            throw new NotImplementedException();
            //var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            //return (subscriptions != null && subscriptions.ToList().Count > 0);
        }

        #endregion
    }
}
