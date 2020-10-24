using System;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Messaging.GcpPubSub.Tests.Integration
{
    [IsIntegration]
    public class PubSubManagerTests
    {
        private readonly IConfiguration _config;
        private readonly string _credentialPath;
        private readonly string _projectId;

        public PubSubManagerTests()
        {
            _config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            _credentialPath = _config["CredentialPath"];
            _projectId = _config["GcpProjectId"];
        }

        /// <summary>Veridy create if not exists generates all the defaults.</summary>
        [Fact]
        public void Test_PubSubManager_CreateTopicIfNotExists()
        {
            // Arrange
            var manager = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _credentialPath,
                ProjectId = _projectId,
            }).EntityManager as PubSubManager;
            var topicName = $"testTopic_creation";

            try
            {
                // Act
                var topicExistsBefore = manager.TopicExists(topicName).GetAwaiter().GetResult();
                var subscriptionExistsBefore = manager.SubscriptionExists($"{topicName}_default").GetAwaiter().GetResult();
                var deadletterTopicExistBefore = manager.TopicExists($"{topicName}_deadletter_default").GetAwaiter().GetResult();
                var deadletterSubscriptionExistsBefore = manager.SubscriptionExists($"{topicName}_deadletter_default").GetAwaiter().GetResult();

                // Main action to test.
                manager.CreateTopicIfNotExists(topicName, true).GetAwaiter().GetResult();

                var topicExistAfter = manager.TopicExists(topicName).GetAwaiter().GetResult();
                var subscriptionExistsAfter = manager.SubscriptionExists($"{topicName}_default").GetAwaiter().GetResult();
                var deadletterTopicExistAfter = manager.TopicExists($"{topicName}_deadletter").GetAwaiter().GetResult();
                var deadletterSubscriptionExistsAfter = manager.SubscriptionExists($"{topicName}_deadletter_default").GetAwaiter().GetResult();

                // Assert
                topicExistsBefore.Should().BeFalse();
                subscriptionExistsBefore.Should().BeFalse();
                deadletterTopicExistBefore.Should().BeFalse();
                deadletterSubscriptionExistsBefore.Should().BeFalse();
                topicExistAfter.Should().BeTrue();
                subscriptionExistsAfter.Should().BeTrue();
                deadletterTopicExistAfter.Should().BeTrue();
                deadletterSubscriptionExistsAfter.Should().BeTrue();
            }
            finally
            {
                manager.DeleteEntity(topicName).GetAwaiter().GetResult();
                manager.DeleteEntity($"{topicName}_deadletter").GetAwaiter().GetResult();
            }
        }

        /// <summary>
        /// Verify topics can be managed appropriately using the entity manager.
        /// Note: This test really needs broken up into individual tests but for time's sake I've kept in one for now.
        /// </summary>
        [Fact]
        public void Test_PubSubManager_CreateEntity()
        {
            // Arrange
            var pubsub = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _credentialPath,
                ProjectId = _projectId,
            });
            var topicName = $"testTopic_{ DateTime.Now.ToEpochTime() }";
            var createEntity = new ReceiverConfig { ProjectId = _projectId, EntityName = topicName };

            // Verify does not exist
            pubsub.EntityManager.EntityExists(topicName).GetAwaiter().GetResult().Should().BeFalse();

            // Create a topic
            pubsub.EntityManager.CreateEntity(createEntity).GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).SubscriptionExists($"{topicName}_default").GetAwaiter().GetResult().Should().BeTrue();

            // Verify the topic exists
            pubsub.EntityManager.EntityExists(topicName).GetAwaiter().GetResult().Should().BeTrue();

            // Attempt to create if not exist
            ((PubSubManager)pubsub.EntityManager).CreateTopicIfNotExists(topicName).GetAwaiter().GetResult();

            // Delete topic
            ((PubSubManager)pubsub.EntityManager).DeleteEntity(topicName).GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).DeleteTopic($"{topicName}_deadletter").GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).DeleteSubscription($"{topicName}_deadletter_default").GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).DeleteSubscription($"{topicName}_deadletter_default").GetAwaiter().GetResult(); // done twice for branch coverage.
            ((PubSubManager)pubsub.EntityManager).DeleteTopic(topicName).GetAwaiter().GetResult(); // done twice for branch coverage.

            // Attempt to create if not exists
            ((PubSubManager)pubsub.EntityManager).CreateTopicIfNotExists(topicName).GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).CreateSubscription(topicName, "OtherSub2").GetAwaiter().GetResult();
            ((PubSubManager)pubsub.EntityManager).CreateSubscription(topicName, "OtherSub2").GetAwaiter().GetResult(); // called twice for coverage.
            ((PubSubManager)pubsub.EntityManager).SubscriptionExists("OtherSub2").GetAwaiter().GetResult().Should().BeTrue();

            // Verify exists
            ((PubSubManager)pubsub.EntityManager).TopicExists(topicName).GetAwaiter().GetResult().Should().BeTrue();

            // Finally... delete
            pubsub.EntityManager.DeleteEntity(topicName).GetAwaiter().GetResult();
            pubsub.EntityManager.DeleteEntity($"{topicName}_deadletter").GetAwaiter().GetResult(); // done twice for branch coverage.

            // Verify does not exist
            ((PubSubManager)pubsub.EntityManager).TopicExists(topicName).GetAwaiter().GetResult().Should().BeFalse();
            ((PubSubManager)pubsub.EntityManager).SubscriptionExists($"{topicName}_default").GetAwaiter().GetResult().Should().BeFalse();
            ((PubSubManager)pubsub.EntityManager).SubscriptionExists("OtherSub2").GetAwaiter().GetResult().Should().BeFalse();
        }
    }
}
