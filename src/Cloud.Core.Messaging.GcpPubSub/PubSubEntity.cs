namespace Cloud.Core.Messaging.GcpPubSub
{
    using System.Collections.Generic;
    
    /// <summary>Class holding a typed body and properties for an PubSub event.</summary>
    /// <typeparam name="T">Type of object held in the body of the entity.</typeparam>
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
        /// Convert the mes
        /// </summary>
        /// <typeparam name="O"></typeparam>
        /// <returns></returns>
        public O GetPropertiesTyped<O>() where O : class, new()
        {
            return Properties.ToObject<O>();
        }

        /// <summary>
        /// Performs an implicit conversion from type T to <see cref="PubSubEntity{T}" />.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>The result of the conversion.</returns>
        public static implicit operator PubSubEntity<T>(T obj)
        {
            return new PubSubEntity<T> { Body = obj };
        }
    }
}
