using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax.ResourceNames;
using Google.Cloud.PubSub.V1;
using Grpc.Core;

namespace Cloud.Core.Messaging.GcpPubSub
{

    public class PubSubManager : IMessageEntityManager
    {
        private readonly PublisherServiceApiClient _publisherServiceApiClient;
        private readonly SubscriberServiceApiClient _receiverSubscriptionClient;
        private readonly string _projectId;

        internal PubSubManager(string projectId, SubscriberServiceApiClient receiverSubscriptionClient, PublisherServiceApiClient publisherServiceApiClient)
        {
            _projectId = projectId;
            _receiverSubscriptionClient = receiverSubscriptionClient;
            _publisherServiceApiClient = publisherServiceApiClient;
        }

        internal async Task CreateTopic(string projectId, string topicName, string deadletterName, string subscriptionName = null, string filter = null)
        {
            if (CreateTopic(projectId, topicName).GetAwaiter().GetResult())
                if (subscriptionName.IsNullOrEmpty())
                    subscriptionName = $"{topicName}_default";

            CreateTopic(projectId, deadletterName).GetAwaiter().GetResult();

            if (subscriptionName.IsNullOrEmpty())
                return;

            await CreateSubscription(projectId, topicName, subscriptionName, deadletterName);
            await CreateSubscription(projectId, deadletterName, deadletterName, null);
        }

        internal async Task<bool> CreateSubscription(string projectId, string topicName, string subscriptionName, string deadletterTopic)
        {
            var subscriptionExists = await SubscriptionExists(projectId, subscriptionName);
            if (!subscriptionExists)
            {
                try
                {
                    var createSubscription = new Subscription
                    {
                        SubscriptionName = new SubscriptionName(projectId, subscriptionName),
                        Topic = new TopicName(projectId, topicName).ToString()
                    };
                    if (!deadletterTopic.IsNullOrEmpty())
                    {
                        createSubscription.DeadLetterPolicy = new DeadLetterPolicy
                        {
                            MaxDeliveryAttempts = 12,
                            DeadLetterTopic = new TopicName(_projectId, deadletterTopic).ToString()
                        };
                    }
                    await _receiverSubscriptionClient.CreateSubscriptionAsync(createSubscription);
                    return true;
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
                {

                }
            }
            return false;
        }

        public async Task<bool> CreateTopic(string projectId, string topicName)
        {
            var topicExists = await TopicExists(projectId, topicName);
            if (!topicExists)
            {
                try
                {
                    await _publisherServiceApiClient.CreateTopicAsync(new TopicName(projectId, topicName));
                    return true;
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
                {

                }
            }
            return false;
        }

        public async Task CreateEntity(IMessageEntityConfig entity)
        {
            //TODO: Consolidate to single interface for entity??
            var psConfig = entity as PubSubEntityConfig;

            //try
            //{
            //    var topic = _publisherServiceApiClient.GetTopic(new GetTopicRequest { Topic = config.Sender.TopicRelativeName });

            //}
            //catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            //{
            //    try
            //    {
            //        _publisherServiceApiClient.CreateTopic(config.TopicRelativeName);
            //    }
            //    catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
            //    {
            //        throw;
            //    }
            //}


            //await _receiverSubscriptionClient.CreateSubscriptionAsync(new Subscription
            //{
            //    SubscriptionName = SubscriptionName.Parse(config.Receiver.EntitySubscriptionName)
            //});
        }

        internal async Task DeleteSubscription(string subscriptionName)
        {
            await _receiverSubscriptionClient.DeleteSubscriptionAsync(new DeleteSubscriptionRequest {
                Subscription = subscriptionName
            });
        }

        internal async Task<bool> SubscriptionExists(string projectId, string subscriptionName)
        {
            try
            {
                var subscription = await _receiverSubscriptionClient.GetSubscriptionAsync(new GetSubscriptionRequest
                {
                    SubscriptionAsSubscriptionName = new SubscriptionName(projectId, subscriptionName),
                });

                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                return false;
            }
        }

        internal async Task<bool> TopicExists(string projectId, string topicName)
        {
            try
            {
                await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { TopicAsTopicName = new TopicName(projectId, topicName) });
                return true;
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {
                return false;
            }
        }

        internal async Task DeleteTopic(string projectId, string topicName)
        {
            try
            {
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(projectId, topicName));
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {

            }
        }

        public async Task DeleteEntity(string entityName)
        {
            try
            {
                var topic = await _publisherServiceApiClient.GetTopicAsync(new TopicName(_projectId, entityName));
                var subs = _receiverSubscriptionClient.ListSubscriptions(new ListSubscriptionsRequest
                {
                    ProjectAsProjectName = new ProjectName(_projectId)
                });
                await _publisherServiceApiClient.DeleteTopicAsync(new TopicName(_projectId, entityName));

                foreach (var subscription in subs)
                {
                    await DeleteSubscription(subscription.Name);
                }
            }
            catch (RpcException e) when (e.StatusCode == StatusCode.NotFound)
            {

            }

        }

        public async Task<bool> EntityExists(string entityName)
        {
            var topic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = entityName });
            return topic.IsNullOrDefault();
        }

        public async Task<decimal> GetReceiverEntityUsagePercentage()
        {
            var topic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            var size = topic.CalculateSize();
            return size;
        }

        public Task<EntityMessageCount> GetReceiverMessageCount()
        {
            throw new NotImplementedException();
        }

        public Task<decimal> GetSenderEntityUsagePercentage()
        {
            throw new NotImplementedException();
        }

        public async Task<EntityMessageCount> GetSenderMessageCount()
        {
            var activeTopic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            var deadLetterTopic = await _publisherServiceApiClient.GetTopicAsync(new GetTopicRequest { Topic = "" });
            return new EntityMessageCount
            {
                ActiveEntityCount = activeTopic.CalculateSize(),
                ErroredEntityCount = deadLetterTopic.CalculateSize()
            };
        }

        public async Task<bool> IsReceiverEntityDisabled()
        {
            var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            var sub = _receiverSubscriptionClient.GetSubscription(new SubscriptionName("", ""));

            return (subscriptions != null && subscriptions.ToList().Count > 0);
        }

        public async Task<bool> IsSenderEntityDisabled()
        {
            var subscriptions = await _publisherServiceApiClient.ListTopicSubscriptionsAsync(new ListTopicSubscriptionsRequest { Topic = "" }).ReadPageAsync(1);
            return (subscriptions != null && subscriptions.ToList().Count > 0);
        }
    }

}
