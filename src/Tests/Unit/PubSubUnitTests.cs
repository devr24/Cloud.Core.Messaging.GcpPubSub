using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Messaging.GcpPubSub;
using Cloud.Core.Testing;
using FluentAssertions;
using Xunit;

namespace Cloud.Core.Messenger.PubSubMessenger.Tests.Unit
{
    [IsUnit]
    public class PubSubUnitTests
    {
        /// <summary>Verify properties can be mapped to a concrete type.</summary>
        [Fact]
        public void Test_PubSubEnity_GetPropertiesTyped()
        {
            // Arrange
            var pubSubEntity = new PubSubEntity<TestClass>
            {
                Body = new TestClass { Property1 = "PropTest" },
                Properties = new Dictionary<string, object>
                {
                    { "Property1", "PropValue" }
                }
            };

            // Act
            var typedProps = pubSubEntity.GetPropertiesTyped<TestClass>();

            // Assert
            typedProps.Should().NotBe(null);
            typedProps.Property1.Should().Be("PropValue");
        }

        /// <summary>Verify properties are automatically set with the correct values.</summary>
        [Fact]
        public void Test_PubSubEntityConfig_AutosetProperties()
        {
            // Arrange / Act
            var config = new PubSubEntityConfig
            {
                ProjectId = "projId",
                EntityName = "entityName",
                EntitySubscriptionName = "entitySubName"
            };
            
            // Assert
            config.TopicRelativeName.Should().Be($"projects/projId/topics/entityName");
            config.DeadLetterEntityName.Should().Be($"entityName_deadletter");
            config.TopicDeadletterRelativeName.Should().Be($"projects/projId/topics/entityName_deadletter");
            config.EntitySubscriptionName.Should().Be($"entitySubName");
        }

        [Fact]
        public void Test_JsonAuthConfig_Validate()
        {
            // Arrange
            var invalidConfig = new JsonAuthConfig();
            var validConfig = new JsonAuthConfig { ProjectId = "test", JsonAuthFile = "test" };

            // Act
            

            // Assert
            Assert.Throws<ValidateException>(() => invalidConfig.ThrowIfInvalid());
            AssertExtensions.DoesNotThrow(() => validConfig.ThrowIfInvalid());
        }

        [Fact]
        public void Test_PubSubConfig_Validate()
        {

        }


        [Fact]
        public void Test_PubSub_ReceiverSetup() { }

        [Fact]
        public void Test_PubSub_SenderSetup() { }

        private class TestClass
        {
            public string Property1 { get; set; }
        }
    }
}
