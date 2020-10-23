using System;
using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Cloud.Core.Messaging.GcpPubSub.Tests.Unit
{
    [IsUnit]
    public class PubSubUnitTests
    {
        /// <summary>Verify properties can be mapped to a concrete type.</summary>
        [Fact]
        public void Test_PubSubEnity_GetPropertiesTyped()
        {
            // Arrange
            var pubSubEntity = new PubSubMessageEntity<TestClass>
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
                EntityName = "entityName"
            };

            // Assert
            config.TopicRelativeName.Should().Be($"projects/projId/topics/entityName");
            config.EntityDeadLetterName.Should().Be($"entityName_deadletter");
            config.TopicDeadletterRelativeName.Should().Be($"projects/projId/topics/entityName_deadletter");
            config.ToString().Length.Should().BeGreaterThan(0);
            config.ProjectId.Should().Be("projId");
        }

        /// <summary>Verify the validation when json auth file path is not set.</summary>
        [Fact]
        public void Test_JsonAuthConfig_Validate()
        {
            // Arrange
            var invalidConfig1 = new PubSubJsonAuthConfig();
            var invalidConfig2 = new PubSubJsonAuthConfig { ProjectId = "test" };
            var validConfig = new PubSubJsonAuthConfig { ProjectId = "test", JsonAuthFile = "test" };
            
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
            config.ToString().Contains("SenderInfo: [NOT SET]").Should().BeTrue();

            // Assert on the sender config validity.
            config.Sender = new SenderConfig();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.Sender.EntityName = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ProjectId = null;
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ToString().Contains("SenderInfo: [NOT SET]").Should().BeFalse();

            // Assert on the receiverConfig config validity.
            config.ReceiverConfig = new ReceiverConfig();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ReceiverConfig.EntityName = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ProjectId = null;
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ToString().Contains("ReceiverInfo: [NOT SET]").Should().BeFalse();
        }

        /// <summary>Confirm receiver is setup as expected.</summary>
        [Fact]
        public void Test_ReceiverConfig_Setup()
        {
            // Arrange
            var receiver = new ReceiverConfig
            {
                EntityName = "entityName",
                EntitySubscriptionName = "test",
                ReadFromErrorEntity = true,
                CreateEntityIfNotExists = false,
                EntityFilter = new KeyValuePair<string, string>("key","value"),
                ProjectId = "projId"
            };

            // Assert
            receiver.TopicRelativeName.Should().Be($"projects/projId/topics/entityName");
            receiver.EntityDeadLetterName.Should().Be($"entityName_deadletter");
            receiver.TopicDeadletterRelativeName.Should().Be($"projects/projId/topics/entityName_deadletter");
            receiver.ToString().Length.Should().BeGreaterThan(0);
            receiver.ProjectId.Should().Be("projId");
            receiver.ReadFromErrorEntity.Should().BeTrue();
        }

        /// <summary>Confirm sender is setup as expected.</summary>
        [Fact]
        public void Test_SenderConfig_Setup()
        {
            // Arrange
            var sender = new SenderConfig
            {
                EntityName = "entityName",
                CreateEntityIfNotExists = false,
                ProjectId = "projId"
            };

            // Assert
            sender.TopicRelativeName.Should().Be($"projects/projId/topics/entityName");
            sender.EntityDeadLetterName.Should().Be($"entityName_deadletter");
            sender.TopicDeadletterRelativeName.Should().Be($"projects/projId/topics/entityName_deadletter");
            sender.ToString().Length.Should().BeGreaterThan(0);
            sender.ProjectId.Should().Be("projId");
        }
        
        /// <summary>Check the add service collection extension configures as expected.</summary>
        [Fact]
        public void Test_ServiceBusMessenger_ServiceCollectionAddSingleton()
        {
            // Accept
            IServiceCollection serviceCollection = new ServiceCollection();

            // Act/Assert
            serviceCollection.AddPubSubSingleton<IReactiveMessenger>(new PubSubConfig { ProjectId = "test" });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IReactiveMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingletonNamed<IMessenger>("key1", new PubSubConfig { ProjectId = "key1" });
            serviceCollection.AddPubSubSingletonNamed<IMessenger>("key2", new PubSubConfig { ProjectId = "key2" });
            serviceCollection.AddPubSubSingletonNamed<IReactiveMessenger>("key3", new PubSubConfig { ProjectId = "key3" });
            serviceCollection.AddPubSubSingletonNamed<IReactiveMessenger>("key4", new PubSubConfig { ProjectId = "key4" });
            serviceCollection.AddPubSubSingleton<IMessenger>(new PubSubConfig { ProjectId = "test1" });
            serviceCollection.AddPubSubSingleton<IReactiveMessenger>(new PubSubConfig { ProjectId = "test2" });
            serviceCollection.AddPubSubSingletonNamed<IMessenger>("key5", new PubSubJsonAuthConfig { ProjectId = "key5", JsonAuthFile = "test" });
            serviceCollection.AddPubSubSingletonNamed<IReactiveMessenger>("key6", new PubSubJsonAuthConfig { ProjectId = "key6", JsonAuthFile = "test" });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(IMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IReactiveMessenger>)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IMessenger>)).Should().BeTrue();

            var resolvedFactory1 = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<IReactiveMessenger>>();
            var resolvedFactory2 = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<IMessenger>>();
            resolvedFactory1["key3"].Should().NotBeNull();
            resolvedFactory1["key4"].Should().NotBeNull();
            resolvedFactory1["test2"].Should().NotBeNull();
            resolvedFactory2["key1"].Should().NotBeNull();
            resolvedFactory2["key2"].Should().NotBeNull();
            resolvedFactory2["test1"].Should().NotBeNull();
            resolvedFactory2["key5"].Should().NotBeNull();
            resolvedFactory1["key6"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingletonNamed<IMessenger>("test", new PubSubJsonAuthConfig { ProjectId = "test", JsonAuthFile = "test" });
            serviceCollection.ContainsService(typeof(IMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<IMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingleton<IReactiveMessenger>(new PubSubJsonAuthConfig { ProjectId = "test", JsonAuthFile = "test" });
            serviceCollection.ContainsService(typeof(IReactiveMessenger)).Should().BeTrue();
            serviceCollection.Clear();
        }

        /// <summary>Verify the methods that should throw not implemented exceptions.</summary>
        [Fact]
        public void Test_PubSubMessenger_NotImplemented()
        {
            // Arrange
            using var pubSub = new PubSubMessenger(new PubSubConfig { ProjectId = "test" });
            
            // Act/Assert
            // Messenger methods.
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.Defer(new[] {""}, null));
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.ReceiveDeferredBatch<string>(new List<long> { 1 }));
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.ReceiveDeferredBatchEntity<string>(new List<long> { 1 }));

            // Manager methods.
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.GetReceiverEntityUsagePercentage());
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.GetSenderEntityUsagePercentage());
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.GetReceiverMessageCount());
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.GetSenderMessageCount());
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.IsReceiverEntityDisabled());
            Assert.ThrowsAsync<NotImplementedException>(async () => await pubSub.EntityManager.IsSenderEntityDisabled());

            pubSub.Dispose();
        }

        /// <summary>Verify a message is abandoned as expected.</summary>
        [Fact]
        public void Test_PubSubMessenger_Abandon()
        {
            // Arrange
            using var pubSub = new PubSubMessenger(new PubSubConfig { ProjectId = "test" });
            var test = "test";

            // Act
            pubSub.Messages.AddOrUpdate(test, null);
            pubSub.Abandon(test).GetAwaiter().GetResult();
            pubSub.Abandon<object>(null).GetAwaiter().GetResult(); // done for branch coverage.

            // Act/Assert
            pubSub.Messages.ContainsKey(test).Should().BeFalse();
        }

        /// <summary>Verify a message entity is abandoned as expected.</summary>
        [Fact]
        public void Test_PubSubMessenger_AbandonMessageEntity()
        {
            // Arrange
            using var pubSub = new PubSubMessenger(new PubSubConfig { ProjectId = "test" });
            var test = new PubSubMessageEntity<string> {Body = "test"};

            // Act
            pubSub.Messages.AddOrUpdate(test.Body, null);
            pubSub.Abandon(test).GetAwaiter().GetResult();

            // Act/Assert
            pubSub.Messages.ContainsKey(test).Should().BeFalse();
        }

        /// <summary>Verify SenderConfig ToString overload works as expected.</summary>
        [Fact]
        public void Test_SenderConfig_ToStringFunctionality()
        {
            // Arrange
            var config = new SenderConfig
            {
                EntityName = "entityName",
                CreateEntityIfNotExists = true,
                ProjectId = "12345"
            };

            // Act
            var str = config.ToString();

            // Assert
            str.Should().Be("EntityName: entityName, EntityDeadLetterName: entityName_deadletter, CreateEntityIfNotExists: True");
        }

        /// <summary>Verify ReceiverConfig ToString overload works as expected.</summary>
        [Fact]
        public void Test_ReceiverConfig_ToStringFunctionality()
        {
            // Arrange
            var config = new ReceiverConfig
            {
                EntityName = "entityName",
                EntitySubscriptionName = "subscriptionName",
                CreateEntityIfNotExists = true,
                EntityFilter = new KeyValuePair<string, string>("testkey", "testfilter"),
                ProjectId = "12345",
                ReadFromErrorEntity = true
            };

            // Act
            var str = config.ToString();

            // Assert
            str.Should().Be("EntityName: entityName, EntityDeadLetterName: entityName_deadletter, EntitySubscriptionName: subscriptionName, " +
                            "EntityDeadLetterSubscriptionName: entityName_deadletter_default, EntityFilter: [testkey, testfilter], CreateEntityIfNotExists: True, ReadFromErrorEntity: True");
        }

        /// <summary>Verify PubSubEntityConfig ToString overload works as expected when sender and receiver are not set.</summary>
        [Fact]
        public void Test_PubSubEntityConfig_ToStringFunctionalityNotSet()
        {
            // Arrange
            var config = new PubSubConfig()
            {
                ProjectId = "12345"
            };

            // Act
            var str = config.ToString();

            // Assert
            str.Should().Be("ProjectId:12345, ReceiverInfo: [NOT SET], SenderInfo: [NOT SET]");
        }

        /// <summary>Verify PubSubConfig ToString overload works as expected.</summary>
        [Fact]
        public void Test_PubSubConfig_ToStringFunctionalityNotSet()
        {
            // Arrange
            var config = new PubSubConfig
            {
                ProjectId = "12345",
                ReceiverConfig = new ReceiverConfig
                {
                    EntityName = "entityName",
                    EntitySubscriptionName = "subscriptionName",
                    CreateEntityIfNotExists = true,
                    EntityFilter = new KeyValuePair<string, string>("testkey", "testfilter"),
                    ProjectId = "12345",
                    ReadFromErrorEntity = true
                }, 
                Sender = new SenderConfig
                {
                    EntityName = "entityName",
                    CreateEntityIfNotExists = true,
                    ProjectId = "12345"
                }
            };

            // Act
            var str = config.ToString();

            // Assert
            str.Should().Be("ProjectId:12345, ReceiverInfo: EntityName: entityName, EntityDeadLetterName: entityName_deadletter, " +
                            "EntitySubscriptionName: subscriptionName, EntityDeadLetterSubscriptionName: entityName_deadletter_default, " +
                            "EntityFilter: [testkey, testfilter], CreateEntityIfNotExists: True, ReadFromErrorEntity: True, SenderInfo: EntityName: entityName, " +
                            "EntityDeadLetterName: entityName_deadletter, CreateEntityIfNotExists: True");
        }

        private class TestClass
        {
            public string Property1 { get; set; }
        }
    }
}
