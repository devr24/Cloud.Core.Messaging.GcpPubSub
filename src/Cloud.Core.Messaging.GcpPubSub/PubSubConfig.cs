﻿namespace Cloud.Core.Messaging.GcpPubSub
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using Validation;

    /// <summary>
    /// Class PubSubEntityConfig.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class PubSubEntityConfig : AttributeValidator
    {
        /// <summary>
        /// Gets or sets the project identifier.
        /// </summary>
        /// <value>The project identifier.</value>
        public string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity.
        /// </summary>
        /// <value>The name of the entity.</value>
        [Required]
        public string EntityName { get; set; }

        /// <summary>
        /// Gets the name of the topic relative.
        /// </summary>
        /// <value>The name of the topic relative.</value>
        public string TopicRelativeName => $"projects/{ProjectId}/topics/{EntityName}";

        /// <summary>
        /// Gets the name of the topic dead-letter relative.
        /// </summary>
        /// <value>The name of the topic dead-letter relative.</value>
        public string TopicDeadletterRelativeName => $"projects/{ProjectId}/topics/{DeadLetterEntityName}";

        /// <summary>
        /// Gets the name of the dead letter entity.
        /// </summary>
        /// <value>The name of the dead letter entity.</value>
        public string DeadLetterEntityName => $"{EntityName}_deadletter";
    }

    /// <summary>
    /// Class GCP PubSub Config.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class ReceiverConfig : PubSubEntityConfig, IMessageEntityConfig
    {
        private string _entityName;

        /// <summary>
        /// Gets or sets the project identifier.
        /// </summary>
        /// <value>The project identifier.</value>
        [Required]
        internal new string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity to receive from.
        /// </summary>
        /// <value>The name of the entity to receive from.</value>
        [Required]
        public new string EntityName 
        {
            get => _entityName;
            set {
                if (EntitySubscriptionName.IsNullOrEmpty())
                    EntitySubscriptionName = $"{value}_default"; 
                _entityName = value;
            }
        }

        /// <summary>
        /// Gets or sets the entity subscription to receive from (if using topics, otherwise this remains null when using queues as its not applicable).
        /// </summary>
        /// <value>The entity subscription.</value>
        public string EntitySubscriptionName { get; set; }

        /// <summary>
        /// Gets or sets the entity filter that's applied if using a topic.
        /// </summary>
        /// <value>The entity filter.</value>
        public KeyValuePair<string, string>? EntityFilter { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to [create the receiverConfig entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [read error (dead-letter) topic].
        /// </summary>
        /// <value><c>true</c> if [read error queue]; otherwise, <c>false</c>.</value>
        public bool ReadFromErrorEntity { get; set; }
    }

    /// <summary>
    /// Class GCP PubSub Config.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class SenderConfig : PubSubEntityConfig
    {
        /// <summary>
        /// Gets or sets the project identifier.
        /// </summary>
        /// <value>The project identifier.</value>
        [Required]
        internal new string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to [create the receiverConfig entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; }
    }

    public class PubSubJsonAuthConfig : PubSubConfig
    {
        [Required]
        public string JsonAuthFile { get; set; }
    }

    /// <summary>
    /// Class GCP PubSub Config.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class PubSubConfig : AttributeValidator
    {
        private string _projectId;
        private ReceiverConfig _receiverConfig;
        private SenderConfig _sender;

        /// <summary>Gets or sets the project identifier.</summary>
        /// <value>The project identifier.</value>
        [Required]
        public string ProjectId { get => _projectId;
            set
            {
                _projectId = value;
                if (_receiverConfig != null)
                    _receiverConfig.ProjectId = _projectId;
                if (_sender != null)
                    _sender.ProjectId = _projectId;
            }
        }

        /// <summary>
        /// Gets or sets the receiverConfig configuration.
        /// </summary>
        /// <value>The receiverConfig config.</value>
        public ReceiverConfig ReceiverConfig 
        {
            get
            {
                if (_receiverConfig != null)
                    _receiverConfig.ProjectId = ProjectId;
                return _receiverConfig;
            }
            set
            {
                _receiverConfig = value;
                if (_receiverConfig != null)
                    _receiverConfig.ProjectId = ProjectId;
            }
        }

        /// <summary>
        /// Gets or sets the sender configuration.
        /// </summary>
        /// <value>The sender config.</value>
        public SenderConfig Sender
        {
            get
            {
                if (_sender != null)
                    _sender.ProjectId = ProjectId;
                return _sender;
            }
            set
            {
                _sender = value;
                if (_sender != null)
                    _sender.ProjectId = ProjectId;
            }
        }

        /// <inheritdoc cref="ValidateResult"/>
        public override ValidateResult Validate(IServiceProvider serviceProvider = null)
        {
            // Validate receiverConfig config if set.
            if (ReceiverConfig != null)
            {
                var validationResult = ReceiverConfig.Validate();
                if (!validationResult.IsValid)
                    return validationResult;
            }

            // Validate the sender config if its been set.
            if (Sender != null)
            {
                var validationResult = Sender.Validate();
                if (!validationResult.IsValid)
                    return validationResult;
            }

            return base.Validate(serviceProvider);
        }

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        /// <inheritdoc />
        public override string ToString()
        {
            return $"ProjectId:{ProjectId}{Environment.NewLine}ReceiverInfo: {(ReceiverConfig == null ? "[NOT SET]" : ReceiverConfig.ToString())}" +
                   $"{Environment.NewLine}SenderInfo: {(Sender == null ? "[NOT SET]" : Sender.ToString())}";
        }
    }
}
