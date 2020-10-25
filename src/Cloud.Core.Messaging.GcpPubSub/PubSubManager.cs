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
        /// <param name="deadletterSubscriptionName">Name of the dead-letter subscription.</param>
        /// <param name="filter">The filter for the subscription.</param>
        public async Task CreateTopicDefaults(string topicName, string deadletterName = null, string subscriptionName = null, string deadletterSubscriptionName = null, string filter = null)
        {
            // Create the topic and the dead-letter equivalent.
            if (!deadletterName.IsNullOrEmpty())
            {
                if (await CreateTopicIfNotExists(deadletterName) && !deadletterSubscriptionName.IsNullOrEmpty())
                {
                    await CreateSubscription(deadletterName, deadletterSubscriptionName);
                }
            }

            if (await CreateTopicIfNotExists(topicName))
            {
                // If a subscription has been requested for creation, create. Along with dead-letter subscription.
                if (!subscriptionName.IsNullOrEmpty())
                    await CreateSubscription(topicName, subscriptionName, filter, deadletterName);
            }
        }

        /// <summary>
        /// Creates the specified subscription.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <param name="filter">The filter.</param>
        /// <param name="deadletterName">Name of the dead-letter topic.</param>
        /// <returns>
        ///   <c>true</c> if created, <c>false</c> if already exists.</returns>
        public async Task<bool> CreateSubscription(string topicName, string subscriptionName, string filter = null, string deadletterName = null)
        {
            var createSubscription = new Subscription
            {
                SubscriptionName = new SubscriptionName(_projectId, subscriptionName),
                Topic = new TopicName(_projectId, topicName).ToString(),
            };

            if (!filter.IsNullOrEmpty())
            {
                createSubscription.Filter = filter;
            }

            if (!deadletterName.IsNullOrEmpty())
            {
                createSubscription.DeadLetterPolicy = new DeadLetterPolicy
                {
                    MaxDeliveryAttempts = 12,
                    DeadLetterTopic = new TopicName(_projectId, deadletterName).ToString()
                };
            }

            try
            {
                // Create topic subscription.
                await _receiverSubscriptionClient.CreateSubscriptionAsync(createSubscription);
                return true;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
                return false;
            }
        }

        /// <summary>
        /// Deletes the specified subscription.
        /// </summary>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns><c>true</c> if deleted, <c>false</c> if not found.</returns>
        public async Task<bool> DeleteSubscription(string subscriptionName)
        {
            try
            {
                // Delete the specified subscription.
                await _receiverSubscriptionClient.DeleteSubscriptionAsync(new DeleteSubscriptionRequest {
                    SubscriptionAsSubscriptionName = new SubscriptionName(_projectId, subscriptionName),
                });
                return true;
            }
            catch (RpcException e) when(e.StatusCode == StatusCode.NotFound)
            {
                // Not found error.
                return false;
            }
        }

        /// <summary>
        /// Check if the subscription exists.
        /// </summary>
        /// <param name="subscriptionName">Name of the subscription.</param>
        /// <returns><c>true</c> if exists, <c>false</c> if not found.</returns>
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
        /// <returns><c>true</c> if topic exists, <c>false</c> otherwise.</returns>
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

        /// <summary>
        /// Deletes the specified topic.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns><c>true</c> if deleted, <c>false</c> if not found.</returns>
        public async Task<bool> DeleteTopic(string topicName)
        {
            try
            {
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(_projectId, topicName));
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                // If it wasn't found, then do nothing as non-existence was the desired result.
                return false;
            }
        }

        /// <summary>
        /// Creates the topic if it does not exist.
        /// </summary>
        /// <param name="topicName">Name of the topic.</param>
        /// <returns><c>true</c> if created, <c>false</c> if not (already exists).</returns>
        public async Task<bool> CreateTopicIfNotExists(string topicName)
        {
            try
            {
                await _publisherServiceApiClient.CreateTopicAsync(new TopicName(_projectId, topicName));
                return true;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
                // If it already exists do nothing as that was the desired outcome.
                return false;
            }
        }

        /// <summary>
        /// Creates the topic, along with default subscription/dead-letter topic/subscription, if the topic does not exists.
        /// </summary>
        /// <param name="topicName">Name of the topic to create if not already exists.</param>
        /// <param name="createDefaults">If set to <c>true</c> [create default subscription, as well dead-letter topic/subscription].</param>
        /// <returns><c>true</c> if created, <c>false</c> if not.</returns>
        public async Task<bool> CreateTopicIfNotExists(string topicName, bool createDefaults)
        {
            // If the topic does not exist, create the topic along with the default subscriptions and dead-letter topic/subscription.
            if (!await TopicExists(topicName))
            {
                // Build defaults.
                var defaultConfig = new ReceiverConfig { EntityName = topicName };

                // Set depending on flag.
                string subscription = createDefaults ? defaultConfig.EntitySubscriptionName : null;
                string deadletterName = createDefaults ? defaultConfig.EntityDeadLetterName : null;
                string deadletterSubscription = createDefaults ? defaultConfig.EntityDeadLetterSubscriptionName : null;

                // Create...
                await CreateTopicDefaults(topicName, deadletterName, subscription, deadletterSubscription);

                return true;
            }

            return false;
        }


        #region Interface Methods

        /// <summary>Creates the PubSub entity.</summary>
        /// <param name="entity">The entity.</param>
        public async Task CreateEntity(IMessageEntityConfig entity)
        {
            if (entity is PubSubEntityConfig psConfig)
            {
                string subName = null;
                string deadletterSubName = null;
                string filter = null;

                // Cast to config that has subscriptions to see if they are set.
                if (psConfig is ReceiverConfig receiver)
                {
                    subName = receiver.EntitySubscriptionName;
                    deadletterSubName = receiver.EntityDeadLetterSubscriptionName;
                    filter = receiver.EntityFilter.GetValueOrDefault().Value;
                }

                // Create topic plus subscriptions if set.
                await CreateTopicDefaults(psConfig.EntityName, psConfig.EntityDeadLetterName, subName, deadletterSubName, filter);
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
                
                foreach (var subscription in subs)
                {
                    // Delete subscriptions associated with the topic.
                    if (subscription.TopicAsTopicName == new TopicName(_projectId, entityName))
                        await DeleteSubscription(subscription.SubscriptionName.SubscriptionId);
                }

                // Begin by deleting the topic.
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(_projectId, entityName));
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
        /// [NOT IMPLEMENTED]
        /// Gets the receiver entity usage percentage. 1.0 represents 100%.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<decimal> GetReceiverEntityUsagePercentage()
        {
            throw new NotImplementedException();
            //var topic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            //var size = topic.CalculateSize();
            //return size;
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Gets the receiver active and errored message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<EntityMessageCount> GetReceiverMessageCount()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Gets the sender entity usage percentage. 1.0 represents 100%.
        /// </summary>
        /// <returns>Task&lt;System.Double&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<decimal> GetSenderEntityUsagePercentage()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Gets the sender active and errored message count.
        /// </summary>
        /// <returns>Task&lt;EntityMessageCount&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
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
        /// [NOT IMPLEMENTED]
        /// Determines whether [receiver entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<bool> IsReceiverEntityDisabled()
        {
            throw new NotImplementedException();
            //var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            //var sub = _receiverSubscriptionClient.GetSubscription(new SubscriptionName("", ""));

            //return (subscriptions != null && subscriptions.ToList().Count > 0);
        }

        /// <summary>
        /// [NOT IMPLEMENTED]
        /// Determines whether [sender entity is disabled].
        /// </summary>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="NotImplementedException">This method has not been implemented for this provider.</exception>
        public Task<bool> IsSenderEntityDisabled()
        {
            throw new NotImplementedException();
            //var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            //return (subscriptions != null && subscriptions.ToList().Count > 0);
        }

        #endregion
    }
}
