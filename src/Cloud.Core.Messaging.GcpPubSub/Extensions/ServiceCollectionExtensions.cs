// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Messaging.GcpPubSub;

    /// <summary>Class Service Collection extensions.</summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Add instance of Gcp PubSub, with PubSubConfig, to the service collection with the NamedInstance factory.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the PubSub singleton.</param>
        /// <param name="config">Configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddPubSubSingletonNamed<T>(this IServiceCollection services, string key, PubSubConfig config)
            where T : IMessageOperations
        {
            var pubSubInstance = new PubSubMessenger(config);

            if (!key.IsNullOrEmpty())
            {
                pubSubInstance.Name = key;
            }

            services.AddSingleton(typeof(T), pubSubInstance);
            services.AddSingleton(pubSubInstance);

            // Ensure there's a NamedInstance factory to allow named collections of the messenger.
            services.AddFactoryIfNotAdded<T>();
            
            return services;
        }

        /// <summary>
        /// Add instance of Gcp PubSub, with JsonAuth config, to the service collection with the NamedInstance factory.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the PubSub singleton.</param>
        /// <param name="config">Configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddPubSubSingletonNamed<T>(this IServiceCollection services, string key, PubSubJsonAuthConfig config)
            where T : IMessageOperations
        {
            var pubSubInstance = new PubSubMessenger(config);

            if (!key.IsNullOrEmpty())
            {
                pubSubInstance.Name = key;
            }

            services.AddSingleton(typeof(T), pubSubInstance);
            services.AddSingleton(pubSubInstance);

            // Ensure there's a NamedInstance factory to allow named collections of the messenger.
            services.AddFactoryIfNotAdded<T>();

            return services;
        }

        /// <summary>
        /// Adds the service bus singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>ServiceCollection.</returns>
        /// <exception cref="InvalidOperationException">Problem occurred while configuring Service Bus Manager Identify config</exception>
        public static IServiceCollection AddPubSubSingleton<T>(this IServiceCollection services, PubSubConfig config)
            where T : IMessageOperations
        {
            return services.AddPubSubSingletonNamed<T>(null, config);
        }
        /// <summary>
        /// Adds the service bus singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The json auth configuration.</param>
        /// <returns>ServiceCollection.</returns>
        /// <exception cref="InvalidOperationException">Problem occurred while configuring Service Bus Manager Identify config</exception>
        public static IServiceCollection AddPubSubSingleton<T>(this IServiceCollection services, PubSubJsonAuthConfig config)
            where T : IMessageOperations
        {
            return services.AddPubSubSingletonNamed<T>(null, config);
        }
    }
}
