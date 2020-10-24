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
        /// Add instance of Gcp PubSub (with messaging interface), with PubSubConfig, to the service collection with the NamedInstance factory.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the PubSub singleton.</param>
        /// <param name="config">Configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddPubSubSingletonNamed<T>(this IServiceCollection services, string key, PubSubConfig config)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, key, new PubSubMessenger(config));
        }

        /// <summary>
        /// Add instance of Gcp PubSub (no interface), with PubSubConfig, to the service collection with the NamedInstance factory.
        /// </summary>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the PubSub singleton.</param>
        /// <param name="config">Configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddPubSubSingletonNamed(this IServiceCollection services, string key, PubSubConfig config)
        {
            return AddNamedInstance<PubSubMessenger>(services, key, new PubSubMessenger(config));
        }

        /// <summary>
        /// Add instance of Gcp PubSub (with messaging interface), with JsonAuth config, to the service collection with the NamedInstance factory.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the PubSub singleton.</param>
        /// <param name="config">Configuration</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddPubSubSingletonNamed<T>(this IServiceCollection services, string key, PubSubJsonAuthConfig config)
            where T : IMessageOperations
        {
            return AddNamedInstance<T>(services, key, new PubSubMessenger(config));
        }

        /// <summary>
        /// Adds the Gcp Pub/Sub singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>ServiceCollection.</returns>
        public static IServiceCollection AddPubSubSingleton<T>(this IServiceCollection services, PubSubConfig config)
            where T : IMessageOperations
        {
            return services.AddPubSubSingletonNamed<T>(null, config);
        }
        /// <summary>
        /// Adds the Gcp Pub/Sub singleton.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">The services.</param>
        /// <param name="config">The json auth configuration.</param>
        /// <returns>ServiceCollection.</returns>
        public static IServiceCollection AddPubSubSingleton<T>(this IServiceCollection services, PubSubJsonAuthConfig config)
            where T : IMessageOperations
        {
            return services.AddPubSubSingletonNamed<T>(null, config);
        }

        /// <summary>
        /// Adds the Gcp Pub/Sub singleton with Json Auth Config.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddPubSubSingleton(this IServiceCollection services, PubSubJsonAuthConfig config)
        {
            return services.AddPubSubSingletonNamed<PubSubMessenger>(null, config);
        }

        /// <summary>
        /// Adds the Gcp Pub/Sub singleton.
        /// </summary>
        /// <param name="services">The services.</param>
        /// <param name="config">The configuration.</param>
        /// <returns>IServiceCollection.</returns>
        public static IServiceCollection AddPubSubSingleton(this IServiceCollection services, PubSubConfig config)
        {
            return services.AddPubSubSingletonNamed<PubSubMessenger>(null, config);
        }

        private static IServiceCollection AddNamedInstance<T>(IServiceCollection services, string key, PubSubMessenger instance)
            where T : INamedInstance
        {
            if (!key.IsNullOrEmpty())
            {
                instance.Name = key;
            }

            services.AddSingleton(typeof(T), instance);

            // Ensure there's a NamedInstance factory to allow named collections of the messenger.
            services.AddFactoryIfNotAdded<T>();

            return services;
        }
    }
}
