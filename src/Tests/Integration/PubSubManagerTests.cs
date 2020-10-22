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

        /// <summary>Verify topics can be managed appropriately using the entity manager.</summary>
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
            var createEntity = new ReceiverConfig { ProjectId = _projectId, EntityName = topicName, EntitySubscriptionName = "" };

            // Verify does not exist
            pubsub.EntityManager.EntityExists(topicName).GetAwaiter().GetResult().Should().BeFalse();

            // Create a topic
            pubsub.EntityManager.CreateEntity(createEntity).GetAwaiter().GetResult();

            // Verify the topic exists
            pubsub.EntityManager.EntityExists(topicName).GetAwaiter().GetResult().Should().BeTrue();

            // Attempt to create if not exist
            ((PubSubManager)pubsub.EntityManager).CreateTopicIfNotExists(topicName).GetAwaiter().GetResult();

            // Delete topic
            ((PubSubManager)pubsub.EntityManager).DeleteTopic(topicName).GetAwaiter().GetResult();

            // Attemt to create if not exists
            ((PubSubManager)pubsub.EntityManager).CreateTopicIfNotExists(topicName).GetAwaiter().GetResult();

            // Verify exists
            ((PubSubManager)pubsub.EntityManager).TopicExists(topicName).GetAwaiter().GetResult().Should().BeTrue();

            // Finally... delete
            pubsub.EntityManager.DeleteEntity(topicName).GetAwaiter().GetResult();

            // Verify does not exist
            ((PubSubManager)pubsub.EntityManager).TopicExists(topicName).GetAwaiter().GetResult().Should().BeFalse();
        }
    }
}
