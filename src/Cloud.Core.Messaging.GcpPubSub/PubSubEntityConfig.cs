namespace Cloud.Core.Messaging.GcpPubSub
{
    public class PubSubEntityConfig : IEntityConfig
    {
        public string EntityName { get; set; }
        public string EntitySubscriptionName { get; set; }
    }

}
