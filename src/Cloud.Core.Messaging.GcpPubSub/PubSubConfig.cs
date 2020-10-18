using Google.Cloud.PubSub.V1;

namespace Cloud.Core.Messaging.GcpPubSub
{
    public class PubSubConfig
    {
        public string ProjectId { get; set; }
        public string TopicId { get; set; }
        public string DeadLetterTopicId { get; set; } = "$Default";
        public string SubscriptionName { get; set; }
        public string TopicRelativeName => GetTopicName(ProjectId, TopicId);
        public string DeadLetterTopicRelativeName => GetTopicName(ProjectId, DeadLetterTopicId);

        internal string GetTopicName(string projectId, string topicId)
        {
            return TopicName.FromProjectTopic(projectId, topicId).ToString();
        }
    }

}
