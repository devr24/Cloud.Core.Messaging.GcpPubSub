using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Cloud.Core.Testing;
using Cloud.Core.Testing.Lorem;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Xunit;

namespace Cloud.Core.Messaging.GcpPubSub.Tests.Performance
{
    [IsPerformance]
    public class PerfTestsFixture
    {
        [Fact]
        public void Test_PerformanceTesting_Run()
        {
            var summary = BenchmarkRunner.Run<PerformanceTesting>();
            //var str = JsonConvert.SerializeObject(summary);
            //Console.WriteLine(str);
            var results = new Dictionary<string, PerfResult>();
            foreach (var row in summary.Table.FullContent)
            {
                var methodName = row[0];
                var mean = row[1];
                var error = row[2];
                var stdDev = row[3];
                var median = row[4];

                results.Add(methodName, new PerfResult
                {
                    Mean = mean,
                    Error = error,
                    Median = median,
                    StandardDeviation = stdDev
                });
            }
            Console.WriteLine(JsonConvert.SerializeObject(results));
        }


    }

    public class PerfResult
    {
        public string Mean { get; set; }
        public string Median { get; set; }
        public string Error { get; set; }
        public string StandardDeviation { get; set; }
    }

    [MemoryDiagnoser]
    public class PerformanceTesting : IDisposable
    {
        public readonly string TestTopicName = $"perftest_{DateTime.Now.ToEpochTime()}";

        public PubSubMessenger Messenger { get; }

        public PubSubManager Manager { get; }

        public PerformanceTesting()
        {
            var config = new ConfigurationBuilder().AddJsonFile("appSettings.json").Build();
            Messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
            {
                JsonAuthFile = config["CredentialPath"],
                ProjectId = config["GcpProjectId"],
            });
            Manager = Messenger.EntityManager as PubSubManager;
            Manager.CreateEntity(new ReceiverConfig { EntityName = TestTopicName }).GetAwaiter().GetResult();

            // Ensure there are messages to receive.
            var sentences = Lorem.GetSentences(2000);
            Messenger.SendBatch(TestTopicName, sentences).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_CreateTopic()
        {
            Manager.CreateTopicDefaults("perftest_createtopic").GetAwaiter().GetResult();
        }

        [Benchmark]
        public async Task Test_Performance_CreateSubscription()
        {
            await Manager.CreateSubscription(TestTopicName, "perftest_createsubscription");
        }

        [Benchmark]
        public void Test_Performance_CreateTopicIfNotExists_DoesNotExist()
        {
            Manager.CreateTopicIfNotExists("perftest_newtopic").GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_CreateTopicIfNotExists_DoesExist()
        {
            Manager.CreateTopicIfNotExists(TestTopicName).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_Send100InvidualMessages()
        {
            var sentences = Lorem.GetSentences(100);
            foreach (var sentence in sentences)
            {
                Messenger.Send(TestTopicName, sentence).GetAwaiter().GetResult();
            }
        }

        [Benchmark]
        public void Test_Performance_Send1InvidualMessage()
        {
            var sentence = Lorem.GetSentence();
            Messenger.Send(TestTopicName, sentence).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_Send1000MessagesInBatchesOf100()
        {
            var sentences = Lorem.GetSentences(1000);
            Messenger.SendBatch(TestTopicName, sentences).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_Receive1000MessagesInBatch()
        {
            Messenger.ReceiveBatch<string>($"{TestTopicName}_default", 1000).GetAwaiter().GetResult();
        }

        [Benchmark]
        public void Test_Performance_Receive100MessagesIndividually()
        {
            for (int i = 0; i < 100; i++)
            {
                Messenger.ReceiveOne<string>($"{TestTopicName}_default").GetAwaiter().GetResult();
            }
        }

        [Benchmark]
        public void Test_Performance_Receive1Messages()
        {
            Messenger.ReceiveOne<string>($"{TestTopicName}_default").GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            Manager.DeleteEntity(TestTopicName).GetAwaiter().GetResult();
            Manager.DeleteEntity("perftest_createtopic").GetAwaiter().GetResult();
            Manager.DeleteEntity("perftest_createsubscription").GetAwaiter().GetResult();
            Manager.DeleteEntity("perftest_newtopic").GetAwaiter().GetResult();
        }
    }
}
