using System.Collections.Generic;
using Google.Cloud.PubSub.V1;

namespace Cloud.Core.Messaging.GcpPubSub
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PubSubEntity<T> : IMessageEntity<T> where T : class
    {
        /// <summary>
        /// 
        /// </summary>
        public T Body { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IDictionary<string, object> Properties { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="O"></typeparam>
        /// <returns></returns>
        public O GetPropertiesTyped<O>() where O : class, new()
        {
            return Properties.ToObject<O>();
        }

        /// <summary>
        /// Performs an implicit conversion from type T to <see cref="PubsubMessage" />.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>The result of the conversion.</returns>
        public static implicit operator PubSubEntity<T>(T obj)
        {
            return new PubSubEntity<T> { Body = obj };
        }
    }

}
