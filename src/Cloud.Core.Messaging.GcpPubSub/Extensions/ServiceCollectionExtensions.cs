namespace Microsoft.Extensions.DependencyInjection
{
    using System;
    using Cloud.Core;
    using Cloud.Core.Messaging.GcpPubSub;

    /// <summary>Class Service Collection extensions.</summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Add service bus singleton of type T, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="projectId">Project Id where PubSub exists.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddGcpPubSub<T>(this IServiceCollection services, string projectId, ReceiverSetup receiver = null, SenderSetup sender = null)
            where T : IMessageOperations
        {
            return services.AddGcpPubSubNamed<T>(null, projectId, receiver, sender);
        }

        /// <summary>
        /// Add service bus singleton of type T, using named properties (as opposed to passing MsiConfig/ServicePrincipleConfig etc).
        /// Will automatically use MsiConfiguration.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="services">Service collection to extend</param>
        /// <param name="key">Key to identify the named instance of the service bus singleton.</param>
        /// <param name="projectId">Project Id where PubSub exists.</param>
        /// <param name="receiver">Receiver configuration (if any).</param>
        /// <param name="sender">Sender configuration (if any).</param>
        /// <returns>Modified service collection with the IReactiveMessenger, IMessenger and NamedInstanceFactory{T} configured.</returns>
        public static IServiceCollection AddGcpPubSubNamed<T>(this IServiceCollection services, string key, string projectId, ReceiverSetup receiver = null, SenderSetup sender = null)
            where T : IMessageOperations
        {
            var pubSubInstance = new PubSubMessenger(new PubSubConfig { 
                ProjectId = projectId,
                Receiver = receiver,
                Sender = sender
            });

            if (!key.IsNullOrEmpty())
            {
                pubSubInstance.Name = key;
            }

            services.AddSingleton(typeof(T), pubSubInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
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
        public static IServiceCollection AddGcpPubSub<T>(this IServiceCollection services, PubSubConfig config)
            where T : IMessageOperations
        {
            var serviceBusInstance = new PubSubMessenger(config);
            services.AddSingleton(typeof(T), serviceBusInstance);
            services.AddFactoryIfNotAdded<IMessenger>();
            services.AddFactoryIfNotAdded<IReactiveMessenger>();
            return services;
        }
    }
}
