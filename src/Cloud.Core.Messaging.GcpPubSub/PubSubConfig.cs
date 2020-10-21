namespace Cloud.Core.Messaging.GcpPubSub
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using Validation;
    using Google.Cloud.PubSub.V1;

    public class PubSubEntityConfig : IMessageEntityConfig
    {
        public string ProjectId { get; set; }
        public string EntityName { get; set; }
        public string EntitySubscriptionName { get; set; }
        public string TopicRelativeName => $"projects/{ProjectId}/topics/{EntityName}";
        public string TopicDeadletterRelativeName => $"projects/{ProjectId}/topics/{DeadLetterEntityName}";
        public string DeadLetterEntityName => $"{EntityName}_deadletter";
    }

    /// <summary>
    /// Class GCP PubSub Config.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class ReceiverSetup : AttributeValidator 
    {
        private string _entityName;

        /// <summary>
        /// Gets or sets the project identifier.
        /// </summary>
        /// <value>The project identifier.</value>
        [Required]
        internal string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets the name of the entity to receive from.
        /// </summary>
        /// <value>The name of the entity to receive from.</value>
        [Required]
        public string EntityName 
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
        /// Gets or sets a value indicating whether to [create the receiver entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [read error (dead-letter) topic].
        /// </summary>
        /// <value><c>true</c> if [read error queue]; otherwise, <c>false</c>.</value>
        public bool ReadFromErrorEntity { get; set; }

        /// <summary>Gets the full relative name of the topic in GCP pub sub.</summary>
        public string TopicRelativeName => $"projects/{ProjectId}/topics/{EntityName}";

        public string DeadLetterEntityName => $"{EntityName}_deadletter";

        /// <summary>Gets the full relative name of the dead-letter topic in GCP pub sub.</summary>
        public string DeadLetterTopicRelativeName => $"projects/{ProjectId}/topics/{DeadLetterEntityName}";
    }

    /// <summary>
    /// Class GCP PubSub Config.
    /// Implements the <see cref="AttributeValidator" />
    /// </summary>
    /// <seealso cref="AttributeValidator" />
    public class SenderSetup : AttributeValidator
    {
        /// <summary>
        /// Gets or sets the project identifier.
        /// </summary>
        /// <value>The project identifier.</value>
        [Required]
        internal string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets the topic identifier.
        /// </summary>
        /// <value>The topic identifier.</value>
        [Required]
        public string TopicId { get; set; }
        public string DeadLetterEntityName => $"{TopicId}_deadletter";

        /// <summary>Gets the full relative name of the dead-letter topic in GCP pub sub.</summary>
        public string DeadLetterTopicRelativeName => $"projects/{ProjectId}/topics/{DeadLetterEntityName}";

        /// <summary>
        /// Gets or sets a value indicating whether to [create the receiver entity if it does not already exist].
        /// </summary>
        /// <value><c>true</c> if [create entity if not exists]; otherwise, <c>false</c> (don't auto create).</value>
        public bool CreateEntityIfNotExists { get; set; }

        /// <summary>Gets the full relative name of the topic in GCP pub sub.</summary>
        public string TopicRelativeName => new TopicName(ProjectId, TopicId).ToString();
    }

    public class JsonAuthConfig : PubSubConfig
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
        private ReceiverSetup _receiver;
        private SenderSetup _sender;

        /// <summary>Gets or sets the project identifier.</summary>
        /// <value>The project identifier.</value>
        [Required]
        public string ProjectId { get; set; }

        /// <summary>
        /// Gets or sets the receiver configuration.
        /// </summary>
        /// <value>The receiver config.</value>
        public ReceiverSetup Receiver 
        {
            get
            {
                if (_receiver != null)
                    _receiver.ProjectId = ProjectId;
                return _receiver;
            }
            set
            {
                _receiver = value;
                _receiver.ProjectId = ProjectId;
            }
        }

        /// <summary>
        /// Gets or sets the sender configuration.
        /// </summary>
        /// <value>The sender config.</value>
        public SenderSetup Sender
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
                _sender.ProjectId = ProjectId;
            }
        }

        /// <inheritdoc cref="ValidateResult"/>
        public override ValidateResult Validate(IServiceProvider serviceProvider = null)
        {
            // Validate receiver config if set.
            if (Receiver != null)
            {
                var validationResult = Receiver.Validate();
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
            return $"ProjectId:{ProjectId}{Environment.NewLine}ReceiverInfo: {(Receiver == null ? "[NOT SET]" : Receiver.ToString())}" +
                   $"{Environment.NewLine}SenderInfo: {(Sender == null ? "[NOT SET]" : Sender.ToString())}";
        }
    }
}
