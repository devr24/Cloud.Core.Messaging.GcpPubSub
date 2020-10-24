using System;
using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
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

            serviceCollection.AddPubSubSingleton("projectid");
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingleton("projectid", "auth");
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingleton(new PubSubConfig { ProjectId = "test" });
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingleton(new PubSubJsonAuthConfig { ProjectId = "test", JsonAuthFile = "test"});
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingletonNamed("test", new PubSubConfig { ProjectId = "test" });
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            var resolvedFactory = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<PubSubMessenger>>();
            resolvedFactory["test"].Should().NotBeNull();
            serviceCollection.Clear();

            serviceCollection.AddPubSubSingletonNamed("test", new PubSubJsonAuthConfig { JsonAuthFile = "test", ProjectId = "test" });
            serviceCollection.ContainsService(typeof(PubSubMessenger)).Should().BeTrue();
            serviceCollection.ContainsService(typeof(NamedInstanceFactory<PubSubMessenger>)).Should().BeTrue();
            var resolvedFactory0 = serviceCollection.BuildServiceProvider().GetService<NamedInstanceFactory<PubSubMessenger>>();
            resolvedFactory0["test"].Should().NotBeNull();
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
            pubSub.Messages.Count.Should().Be(0);

            pubSub.Dispose(); // done for branch coverage.
        }

        /// <summary>Verify a message entity is abandoned as expected.</summary>
        [Fact]
        public void Test_PubSubMessenger_AbandonMessageEntity1()
        {
            // Arrange
            using var pubSub = new PubSubMessenger(new PubSubConfig { ProjectId = "test" });
            var test = new PubSubMessageEntity<string> {Body = "test"};

            // Act
            pubSub.Messages.AddOrUpdate(test.Body, null);
            pubSub.Abandon(test).GetAwaiter().GetResult();

            // Act/Assert
            pubSub.Messages.ContainsKey(test).Should().BeFalse();
            pubSub.Messages.Count.Should().Be(0);
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

        /// <summary>Verify PubSubJsonAuthConfig ToString overload works as expected.</summary>
        [Fact]
        public void Test_PubSubJsonAuthConfig_ToStringFunctionalityNotSet()
        {
            // Arrange
            var config = new PubSubJsonAuthConfig()
            {
                JsonAuthFile = "fileLocation",
                ProjectId = "12345"
            };

            // Act
            var str = config.ToString();

            // Assert
            str.Should().Be("Auth (jsonFile): fileLocation, ProjectId:12345, ReceiverInfo: [NOT SET], SenderInfo: [NOT SET]");
        }

        /// <summary>Verify errors are thrown when methods are used in an invalid way.</summary>
        [Fact]
        public void Test_PubSubMessenger_InvalidOperation()
        {
            // Arrange
            var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build()["CredentialPath"],
                ProjectId = "12345"
            });

            // Act/Assert
            Assert.Throws<InvalidOperationException>(() => messenger.Receive<object>(null, null));
            Assert.Throws<ArgumentException>(() => messenger.ReceiveOne<object>("").GetAwaiter().GetResult());
            Assert.Throws<InvalidOperationException>(() => messenger.StartReceive<object>());
            Assert.Throws<InvalidOperationException>(() => messenger.ReceiveOne<object>());
            Assert.Throws<InvalidOperationException>(() => messenger.ReceiveBatchEntity<object>().GetAwaiter().GetResult());

            // Additional runs here purely to increase code coverage. Not particularly useful tests but it does confirm functionality.
            messenger.Complete<object>(null).GetAwaiter().GetResult();
            messenger.CompleteAll(new object[] {null}).GetAwaiter().GetResult();
            messenger.Abandon(new object());
            messenger.Error<object>(null).GetAwaiter().GetResult();
            messenger.Error(new object()).GetAwaiter().GetResult();
            var props = messenger.ReadProperties<object>(null);
            props.Should().BeNull();
        }

        private class TestClass
        {
            public string Property1 { get; set; }
        }
    }
}
