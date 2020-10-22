using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            // clean up test data from the database
            Messenger.EntityManager.DeleteEntity(TestTopicName).GetAwaiter().GetResult();
            Messenger.EntityManager.DeleteEntity($"{TestTopicName}_deadletter").GetAwaiter().GetResult();
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
            List<string> msgs;
            var batchSize = 10;
            var lorem = Lorem.GetParagraphs(50);

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
                
                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                // Assert
                if (msgs.Count > 0)
                    msgs.Count.Should().Be(batchSize);

            } while (msgs.Count > 0);
        }

        /// <summary>Verify a batch of messages can be received.</summary>
        [Fact]
        public void Test_PubSubMessenger_ReceiveBatch()
        {
            // Arrange
            List<string> msgs;
            var batchSize = 10;
            var lorem = Lorem.GetParagraphs(51);

            // Act
            _fixture.Messenger.SendBatch(lorem, batchSize).GetAwaiter().GetResult();

            do
            {
                // Receive a batch of messages.
                msgs = _fixture.Messenger.ReceiveBatch<string>(batchSize).GetAwaiter().GetResult();

                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                // Assert
                if (msgs.Count > 1)
                    msgs.Count.Should().Be(batchSize);

            } while (msgs.Count > 0);
        }

        /// <summary>Verify a batch of message entities can be received.</summary>
        [Fact]
        public void Test_PubSubMessenger_ReceiveBatchEntity()
        {
            // Arrange
            List<IMessageEntity<string>> msgs;
            var batchSize = 10;
            var lorem = Lorem.GetParagraphs(51);

            // Act
            _fixture.Messenger.SendBatch(lorem, batchSize).GetAwaiter().GetResult();

            do
            {
                // Receive a batch of messages.
                msgs = _fixture.Messenger.ReceiveBatchEntity<string>(batchSize).GetAwaiter().GetResult();

                // Complete multiple messages at once.
                _fixture.Messenger.CompleteAll(msgs).GetAwaiter().GetResult();

                // Assert
                if (msgs.Count > 1)
                    msgs.Count.Should().Be(batchSize);

            } while (msgs.Count > 0);
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

            var msg = _fixture.Messenger.ReceiveOne<string>();
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
            var lorem = Lorem.GetSentences(10);
            var waitTimer = new Stopwatch();
            var messagesProcessed = 0;
            var processedAfterCancelCount = 0;

            // Act
            _fixture.ReactiveMessenger.Send(lorem).GetAwaiter().GetResult();
            _fixture.ReactiveMessenger.StartReceive<string>().Subscribe((m) =>
            {
                _fixture.Messenger.Complete(m).GetAwaiter().GetResult();
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

            _fixture.Messenger.CancelReceive<string>();

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
            var lorem = Lorem.GetSentences(10);
            var waitTimer = new Stopwatch();
            var messagesProcessed = 0;
            var processedAfterCancelCount = 0;

            // Act
            _fixture.Messenger.Send(lorem).GetAwaiter().GetResult();
            _fixture.Messenger.Receive<string>((m) =>
            {
                _fixture.Messenger.Complete(m).GetAwaiter().GetResult();
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

            _fixture.Messenger.CancelReceive<string>();
            
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

        // Task SendBatch<T>(IEnumerable<T> messages, Func<T, KeyValuePair<string, object>[]> setProps, int batchSize = 100) 

        // void Receive<T>(Action<T> successCallback, Action<Exception> errorCallback, int batchSize = 10) 


        // IObservable<T> StartReceive<T>(int batchSize = 10) 

        // void CancelReceive<T>() 

        // Task UpdateReceiver(string entityName, string entitySubscriptionName = null, bool createIfNotExists = false, KeyValuePair<string, string>? entityFilter = null, string entityDeadLetterName = null)

        // Task Error<T>(T message, string reason = null)
    }
}
