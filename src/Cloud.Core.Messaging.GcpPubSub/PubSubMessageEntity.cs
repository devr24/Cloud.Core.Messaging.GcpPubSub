namespace Cloud.Core.Messaging.GcpPubSub
{
    using System.Collections.Generic;
    
    /// <summary>Class holding a typed body and properties for an PubSub event.</summary>
    /// <typeparam name="T">Type of object held in the body of the entity.</typeparam>
    public class PubSubMessageEntity<T> : IMessageEntity<T> where T : class
    {
        /// <summary>Typed body of the message entity.</summary>
        public T Body { get; set; }

        /// <summary>Collection of properties for the message.</summary>
        public IDictionary<string, object> Properties { get; set; }

        /// <summary>Convert the message to a specific type.</summary>
        /// <typeparam name="O"></typeparam>
        /// <returns></returns>
        public O GetPropertiesTyped<O>() where O : class, new()
        {
            return Properties.ToObject<O>();
        }
    }
}
