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

        /// <summary>Verify the validation when json auth file path is not set.</summary>
        [Fact]
        public void Test_JsonAuthConfig_Validate()
        {
            // Arrange
            var invalidConfig1 = new JsonAuthConfig();
            var invalidConfig2 = new JsonAuthConfig { ProjectId = "test" };
            var validConfig = new JsonAuthConfig { ProjectId = "test", JsonAuthFile = "test" };
            
            // Act/Assert
            Assert.Throws<ValidateException>(() => invalidConfig1.ThrowIfInvalid());
            Assert.Throws<ValidateException>(() => invalidConfig2.ThrowIfInvalid());
            AssertExtensions.DoesNotThrow(() => validConfig.ThrowIfInvalid());
        }

        /// <summary>Verify validation is carried out in the expected manor.</summary>
        [Fact]
        public void Test_PubSubConfig_Validate()
        {
            // Arrange
            var config = new PubSubConfig();

            // Act and Assert - overlap here as it was an easier approach for this use-case.
            // Top level validation.
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());

            // Assert on the sender config validity.
            config.Sender = new SenderSetup();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.Sender.TopicId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ProjectId = null;
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());

            // Assert on the receiver config validity.
            config.Receiver = new ReceiverSetup();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.Receiver.EntityName = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ProjectId = null;
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
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
