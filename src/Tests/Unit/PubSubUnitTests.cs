﻿using System.Collections.Generic;
using Cloud.Core.Exceptions;
using Cloud.Core.Messaging.GcpPubSub;
using Cloud.Core.Testing;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
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
                EntityName = "entityName"
            };

            // Assert
            config.TopicRelativeName.Should().Be($"projects/projId/topics/entityName");
            config.DeadLetterEntityName.Should().Be($"entityName_deadletter");
            config.TopicDeadletterRelativeName.Should().Be($"projects/projId/topics/entityName_deadletter");
            config.ToString().Length.Should().BeGreaterThan(0);
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

            // Assert on the sender config validity.
            config.Sender = new SenderConfig();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.Sender.EntityName = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());
            config.ProjectId = null;
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ProjectId = "test";
            AssertExtensions.DoesNotThrow(() => config.ThrowIfInvalid());

            // Assert on the receiverConfig config validity.
            config.ReceiverConfig = new ReceiverConfig();
            Assert.Throws<ValidateException>(() => config.ThrowIfInvalid());
            config.ReceiverConfig.EntityName = "test";
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
    }
}
