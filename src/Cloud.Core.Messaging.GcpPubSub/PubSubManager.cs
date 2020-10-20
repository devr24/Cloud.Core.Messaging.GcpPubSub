using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Cloud.PubSub.V1;
using Grpc.Core;

namespace Cloud.Core.Messaging.GcpPubSub
{

    public class PubSubManager : IMessageEntityManager
    {
        private readonly PublisherServiceApiClient _publisherServiceApiClient;
        private readonly SubscriberServiceApiClient _receiverSubscriptionClient;

        internal PubSubManager(SubscriberServiceApiClient receiverSubscriptionClient, PublisherServiceApiClient publisherServiceApiClient)
        {
            _receiverSubscriptionClient = receiverSubscriptionClient;
            _publisherServiceApiClient = publisherServiceApiClient;
        }

        public async Task CreateEntity(IEntityConfig config)
        {
            //TODO: Consolidate to single interface for entity??


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

        public async Task DeleteEntity(string entityName)
        {
            await _publisherServiceApiClient.DeleteTopicAsync(entityName);
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
