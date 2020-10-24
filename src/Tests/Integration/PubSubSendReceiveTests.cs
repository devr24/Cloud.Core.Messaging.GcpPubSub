using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cloud.Core.Testing;
using Cloud.Core.Testing.Lorem;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Cloud.Core.Messaging.GcpPubSub.Tests.Integration
{
    public class PubSubTestsFixture : IDisposable
    {
        private readonly IConfiguration _config;

        public IReactiveMessenger ReactiveMessenger { get; }
        public IMessenger Messenger { get; }
        public string ProjectId { get; }
        public string CredentialPath { get; }
        public string TestTopicName { get; }
        public string StreamTopicName => $"{TestTopicName}_stream";
        public string StreamObservableTopicName => $"{TestTopicName}_streamObs";
        public string SecondaryTopicName => $"{TestTopicName}_secondary";
        public string MessageFilterTopic => $"{TestTopicName}_filtering";

        public PubSubTestsFixture()
        {
            TestTopicName = $"testTopic_{DateTime.Now.ToEpochTime()}";
            _config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();

            CredentialPath = _config["CredentialPath"];
            ProjectId = _config["GcpProjectId"];
            var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = CredentialPath,
                ProjectId = ProjectId,
                ReceiverConfig = new ReceiverConfig()
                {
                    EntityName = TestTopicName,
                    CreateEntityIfNotExists = true
                },
                Sender = new SenderConfig()
                {
                    EntityName = TestTopicName,
                    CreateEntityIfNotExists = true
                }
            });
            Messenger = messenger;
            ReactiveMessenger = messenger;
        }

        public void Dispose()
        {
            // Clean up topics.
            Messenger.EntityManager.DeleteEntity(TestTopicName).GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity(StreamTopicName).GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity(StreamObservableTopicName).GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity(SecondaryTopicName).GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity(MessageFilterTopic).GetAwaiter().GetResult();

            // Clean up dead-letter topics.
            Messenger.EntityManager.DeleteEntity($"{TestTopicName}_deadletter").GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity($"{StreamTopicName}_deadletter").GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity($"{StreamObservableTopicName}_deadletter").GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity($"{SecondaryTopicName}_deadletter").GetAwaiter().GetResult();
        }
    }

    [IsIntegration]
    public class PubSubSendReceiveTests : IClassFixture<PubSubTestsFixture>
    {
        private readonly PubSubTestsFixture _fixture;

        public PubSubSendReceiveTests(PubSubTestsFixture fixture)
        {
            _fixture = fixture;
        }

        /// <summary>Verify a sent message can then be received and completed.</summary>
        [Fact]
        public void Test_PubSubMessenger_SendMessage()
        {
            // Arrange
            var lorem = Lorem.GetSentence(5);

            // Act
            _fixture.Messenger.Send(lorem).GetAwaiter().GetResult();

            var msg = _fixture.Messenger.ReceiveOne<string>();

            _fixture.Messenger.Complete(msg).GetAwaiter().GetResult();

            // Assert
            msg.Should().BeEquivalentTo(lorem);
        }

        /// <summary>Verify a message sent with properties can be received with properties intact.</summary>
        [Fact]
        public void Test_PubSubMessenger_ReceiveMessageWithProps()
        {
            // Arrange
            var lorem = Lorem.GetSentence(5);

            // Act
            _fixture.Messenger.Send(lorem, new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string,object>("first", 1),
                new KeyValuePair<string, object>("second", "two")
            }).GetAwaiter().GetResult();

            var msg = _fixture.Messenger.ReceiveOne<string>();
            var props = _fixture.Messenger.ReadProperties(msg);

            _fixture.Messenger.Complete(msg).GetAwaiter().GetResult();

            // Assert
            msg.Should().BeEquivalentTo(lorem);
            props["first"].Should().Be("1");
            props["second"].Should().Be("two");
        }

        /// <summary>Verify a message sent with properties can be received with properties intact.</summary>
        [Fact]
        public void Test_PubSubMessenger_SendMessageEntityWithProps()
        {
            // Arrange
            var lorem = Lorem.GetSentence(5);

            // Act
            _fixture.Messenger.Send(lorem, new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string,object>("first", 1),
                new KeyValuePair<string, object>("second", "two")
            }).GetAwaiter().GetResult();

            var msg = _fixture.Messenger.ReceiveOneEntity<string>();

            _fixture.Messenger.Complete(msg).GetAwaiter().GetResult();

            // Assert
            msg.Body.Should().BeEquivalentTo(lorem);
            msg.Properties["first"].Should().Be("1");
            msg.Properties["second"].Should().Be("two");
        }

        /// <summary>Verify a batch of messages, with properties, can then be sent.</summary>
        [Fact]
        public void Test_PubSubMessenger_SendMessageBatchWithProps()
        {
            // Arrange
            IEnumerable<string> msgs;
            var batchSize = 40;
            var lorem = Lorem.GetParagraphs(50);
            var batchCounter = 0;
            int msgCount;

            // Act
            _fixture.Messenger.SendBatch(lorem, new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string,object>("first", 1),
                new KeyValuePair<string, object>("second", "two")
            }, batchSize).GetAwaiter().GetResult();

            do
            {
                // Receive a batch of messages.
                msgs = _fixture.Messenger.ReceiveBatch<string>(batchSize).GetAwaiter().GetResult();
                msgCount = msgs.Count();

                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                if (msgCount == batchSize)
                    batchCounter++;

            } while (msgCount > 0);
            
            // Assert
            batchCounter.Should().BeGreaterThan(0);
        }

        /// <summary>Verify a batch of messages can be received.</summary>
        [Fact]
        public void Test_PubSubMessenger_ReceiveBatch()
        {
            // Arrange
            IEnumerable<string> msgs;
            var batchSize = 40;
            var lorem = Lorem.GetParagraphs(50);
            var batchCounter = 0;
            int msgCount;

            // Act
            _fixture.Messenger.SendBatch(lorem, batchSize).GetAwaiter().GetResult();

            do
            {
                // Receive a batch of messages.
                msgs = _fixture.Messenger.ReceiveBatch<string>(batchSize).GetAwaiter().GetResult();
                msgCount = msgs.Count();

                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                // Assert
                if (msgCount == batchSize)
                    batchCounter++;

            } while (msgCount > 0);

            // Assert
            batchCounter.Should().BeGreaterThan(0);
        }

        /// <summary>Verify a batch of message entities can be received.</summary>
        [Fact]
        public void Test_PubSubMessenger_ReceiveBatchEntity()
        {
            // Arrange
            IEnumerable<IMessageEntity<string>> msgs;
            var batchSize = 10;
            var lorem = Lorem.GetParagraphs(51);
            int msgCount;

            // Act
            _fixture.Messenger.SendBatch(lorem, batchSize).GetAwaiter().GetResult();

            do
            {
                // Receive a batch of messages.
                msgs = _fixture.Messenger.ReceiveBatchEntity<string>(batchSize).GetAwaiter().GetResult();
                msgCount = msgs.Count();

                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                // Assert
                if (msgCount > 1)
                    msgCount.Should().Be(batchSize);

            } while (msgCount > 0);
        }

        /// <summary>Verify a message can be errored and then picked up from dead-letter topic.</summary>
        [Fact]
        public void Test_PubSubMessenger_ErrorMessage()
        {
            // Arrange
            var lorem = Lorem.GetSentence(5);
            var deadletterReader = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _fixture.CredentialPath,
                ProjectId = _fixture.ProjectId,
                ReceiverConfig = new ReceiverConfig()
                {
                    EntityName = _fixture.TestTopicName,
                    CreateEntityIfNotExists = true,
                    ReadFromErrorEntity = true
                }
            });

            // Act
            _fixture.Messenger.Send(lorem).GetAwaiter().GetResult();

            var msg = _fixture.Messenger.ReceiveOneEntity<string>();
            _fixture.Messenger.Error(msg, "Something went wrong!").GetAwaiter().GetResult();
            var deadletterMsg = deadletterReader.ReceiveOneEntity<string>();

            // Assert
            deadletterMsg.Body.Should().NotBeNullOrEmpty();
            deadletterMsg.Body.Should().BeEquivalentTo(lorem);
            deadletterMsg.Properties["ErrorReason"].Should().NotBeNull();
        }

        // <summary>Verify messages can be streamed using observables.</summary>
        [Fact]
        public void Test_PubSubMessenger_StreamMessagesObservable()
        {
            // Arrange
            var pubsub = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _fixture.CredentialPath,
                ProjectId = _fixture.ProjectId,
                ReceiverConfig = new ReceiverConfig()
                {
                    EntityName = _fixture.StreamObservableTopicName,
                    CreateEntityIfNotExists = true
                },
                Sender = new SenderConfig
                {
                    EntityName = _fixture.StreamObservableTopicName,
                    CreateEntityIfNotExists = true
                }
            });
            var lorem = Lorem.GetSentences(10);
            var waitTimer = new Stopwatch();
            var messagesProcessed = 0;
            var processedAfterCancelCount = 0;

            // Act
            pubsub.Send(lorem).GetAwaiter().GetResult();
            pubsub.StartReceive<string>().Subscribe((m) =>
            {
                pubsub.Complete(m).GetAwaiter().GetResult();
                messagesProcessed++;
                processedAfterCancelCount++;
            }, (e) =>
            {

            });
            waitTimer.Start();

            do
            {
                Task.Delay(500).GetAwaiter().GetResult();
            } while (waitTimer.Elapsed.TotalSeconds < 5);

            pubsub.CancelReceive<string>();

            Task.Delay(500).GetAwaiter().GetResult();

            waitTimer.Reset();
            waitTimer.Start();
            processedAfterCancelCount = 0; // verify no more messages were processed after cancellation.
            do
            {
                Task.Delay(500).GetAwaiter().GetResult();
            } while (waitTimer.Elapsed.TotalSeconds < 5);

            // Assert
            messagesProcessed.Should().BeGreaterOrEqualTo(1);
            processedAfterCancelCount.Should().Be(0);
        }

        /// <summary>Verify messages can be streamed using callbacks.</summary>
        [Fact]
        public void Test_PubSubMessenger_StreamMessages()
        {
            // Arrange
            var pubsub = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _fixture.CredentialPath,
                ProjectId = _fixture.ProjectId,
                ReceiverConfig = new ReceiverConfig
                {
                    EntityName = _fixture.StreamTopicName,
                    CreateEntityIfNotExists = true
                },
                Sender = new SenderConfig
                {
                    EntityName = _fixture.StreamTopicName,
                    CreateEntityIfNotExists = true
                }
            });
            var lorem = Lorem.GetSentences(10);
            var waitTimer = new Stopwatch();
            var messagesProcessed = 0;
            var processedAfterCancelCount = 0;

            // Act
            pubsub.Send(lorem).GetAwaiter().GetResult();
            pubsub.Receive<string>((m) =>
            {
                pubsub.Complete(m).GetAwaiter().GetResult();
                messagesProcessed++;
                processedAfterCancelCount++;
            }, (e) =>
            {

            });
            waitTimer.Start();

            do
            {
                Task.Delay(500).GetAwaiter().GetResult();
            } while (waitTimer.Elapsed.TotalSeconds < 5);

            pubsub.CancelReceive<string>();

            Task.Delay(500).GetAwaiter().GetResult();

            waitTimer.Reset();
            waitTimer.Start();
            processedAfterCancelCount = 0; // verify no more messages were processed after cancellation.
            do
            {
                Task.Delay(500).GetAwaiter().GetResult();
            } while (waitTimer.Elapsed.TotalSeconds < 5);

            // Assert
            messagesProcessed.Should().BeGreaterOrEqualTo(1);
            processedAfterCancelCount.Should().Be(0);
        }

        /// <summary>Verify exception when attempt to send but config is null.</summary>
        [Fact]
        public void Test_PubSubMessenger_SendException()
        {
            // Arrange
            var sender = ((PubSubMessenger)_fixture.Messenger).Config.Sender;
            try
            {
                ((PubSubMessenger)_fixture.Messenger).Config.Sender = null;
                
                // Act/Assert
                Assert.ThrowsAsync<InvalidOperationException>(async () => await _fixture.Messenger.Send("test"));
            }
            finally
            {
                ((PubSubMessenger)_fixture.Messenger).Config.Sender = sender;
            }
        }

        /// <summary>Verify a sent message can then be received and completed.</summary>
        [Fact]
        public void Test_PubSubMessenger_UpdateReceiver()
        {
            // Arrange
            var lorem1 = Lorem.GetSentence(5);
            var lorem2 = Lorem.GetSentence(5);
            PubSubManager manger = _fixture.Messenger.EntityManager as PubSubManager;

            // Act
            // Create topics
            manger.CreateTopicIfNotExists(_fixture.SecondaryTopicName).GetAwaiter().GetResult();
            manger.CreateSubscription(_fixture.SecondaryTopicName, $"{_fixture.SecondaryTopicName}_default").GetAwaiter().GetResult();

            // Send to topics
            _fixture.Messenger.Send(lorem1).GetAwaiter().GetResult();
            ((PubSubMessenger)_fixture.Messenger).Send(_fixture.SecondaryTopicName, lorem2).GetAwaiter().GetResult();
            ((PubSubMessenger)_fixture.Messenger).Send(_fixture.SecondaryTopicName, lorem2, new []{  new KeyValuePair<string, object>("prop1","prop1val")  }).GetAwaiter().GetResult(); // used for branch coverage
            ((PubSubMessenger)_fixture.Messenger).SendBatch(_fixture.SecondaryTopicName, new [] { lorem2 }, new[] { new KeyValuePair<string, object>("prop1", "prop1val") }).GetAwaiter().GetResult(); // used for branch coverage

            // Read from topic 1
            var msg1 = _fixture.Messenger.ReceiveOne<string>();
            _fixture.Messenger.Complete(msg1).GetAwaiter().GetResult();

            // Update receiver to read from topic 2
            _fixture.ReactiveMessenger.UpdateReceiver(_fixture.SecondaryTopicName);
            var msg2 = _fixture.Messenger.ReceiveOne<string>();
            _fixture.Messenger.Complete(msg2).GetAwaiter().GetResult();

            // Reset everything.
            _fixture.ReactiveMessenger.UpdateReceiver(_fixture.TestTopicName);

            // Assert
            msg1.Should().Be(lorem1);
            msg2.Should().Be(lorem2);
        }

        /// <summary>Verify messages can be filtered when sent to subscriptions.</summary>
        [Fact]
        public void Test_PubSubMessenger_MessageFiltering()
        {
            // Arrange
            var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _fixture.CredentialPath,
                ProjectId = _fixture.ProjectId
            });
            var manager = messenger.EntityManager as PubSubManager;

            // Act
            // Create the filter topic for testing.
            manager.CreateTopic(_fixture.MessageFilterTopic, null, "defaultsub").GetAwaiter().GetResult();
            manager.CreateSubscription(_fixture.MessageFilterTopic, "filteredsub", "attributes:pickme").GetAwaiter().GetResult();

            // Send two messages, one that wont be picked up by the filter subscription and the other that
            // will.  The result is two messages to the defaultsub, one to filteredsub
            messenger.Send(_fixture.MessageFilterTopic, "test").GetAwaiter().GetResult();
            messenger.Send(_fixture.MessageFilterTopic, "testfilter", new KeyValuePair<string, object>[]
            {
                new KeyValuePair<string, object>("pickme", "please") 
            }).GetAwaiter().GetResult();

            // Receive from both subscriptions.
            var nonFilteredMessages = messenger.ReceiveBatch<string>("defaultsub", 100).GetAwaiter().GetResult();
            var filteredMessages = messenger.ReceiveBatch<string>("filteredsub", 100).GetAwaiter().GetResult();
            var filteredMessage = messenger.ReceiveOne<string>("filteredsub").GetAwaiter().GetResult();

            // Assert 
            nonFilteredMessages.Count().Should().Be(2);
            filteredMessages.Count().Should().Be(1);
            filteredMessage.Should().NotBe(null);
            filteredMessage.Should().Be("testfilter");
        }

        /// <summary>Very the not implemented methods produce the expected errors.</summary>
        [Fact]
        public void Test_PubSubMessenger_NotImplemented()
        {
            // Arrange
            var pubSub = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = _fixture.CredentialPath,
                ProjectId = _fixture.ProjectId
            });

            // Act/Assert - Manager methods.
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.GetReceiverEntityUsagePercentage().GetAwaiter().GetResult());
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.GetSenderEntityUsagePercentage().GetAwaiter().GetResult());
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.GetReceiverMessageCount().GetAwaiter().GetResult());
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.GetSenderMessageCount().GetAwaiter().GetResult());
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.IsReceiverEntityDisabled().GetAwaiter().GetResult());
            Assert.Throws<NotImplementedException>(() => pubSub.EntityManager.IsSenderEntityDisabled().GetAwaiter().GetResult());

        }
    }
}
