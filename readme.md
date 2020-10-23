# **Cloud.Core.Messaging.GcpPubSub**  
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Messenger.GcpPubSub_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=11) ![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Messaging.GcpPubSub-LineCoverage.png) [![Cloud.Core.Messaging.GcpPubSub package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/10bce412-14c4-4eb3-b2fb-8c0def43f9eb/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=10bce412-14c4-4eb3-b2fb-8c0def43f9eb&preferRelease=true)

<div id="description">

Gcp Pub/Sub Topics implementation of the messaging interfaces provided in Cloud.Core.  Abstracts topic and subscriptions management.

**[Read full Api documentation](https://cloud1core.blob.core.windows.net/docs/Cloud.Core.Messaging.GcpPubSub/api/index.html)**
</div>

## Setup

You will need the following setup to use this package:

1) Google Cloud Platform (GCP) account
2) Instance of GCP Pub/Sub
3) IAM setup for the GCP Pub/Sub and download of credentials json

## Initialisation and Authentication 
When you download your credentials file, there are two options (at the moment) for authenticating to GCP Pub/Sub.  As shown as follows along with initialisation:


## Usage

### Interface with Core
The *Cloud.Core* package contains these public interfaces for messaging (chain shown below)


</div>

The *Cloud.Core* package contains these public interfaces for messaging (chain shown below).  This package implements the releavant interfaces for wrapping a Message Bus.  
The main focus of this package being separate from all the other Google Cloud Platform specific packages is to allow for a layer of abstraction in the calling applications.

The interface also allows the implementation to switch to other available messenger types for other cloud offerings, such as Azure Storage Queue, Azure Service Bus and RabbitMQ.

```csharp
IReactiveMessenger messenger = new PubSubMessenger(new PubSubJsonAuthConfig());
```

Whereas the instantiation could easily be changed to use Google as follows:

```csharp
IReactiveMessenger messenger = new ServiceBusMessenger(new MsiConfig());
```

### Method 1 - set credentials file as Environment Variable
You can add an environment setting called 'GOOGLE_APPLICATION_CREDENTIALS' with a path to the credentials *.json file and then the code will automatically pick these up when running.  The initialisation code would look like this:
```csharp
var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
{
	JsonAuthFile = CredentialPath,
	...
});
```
_Remember to run your code in a context that has permissions to read the environment variable._

### Method 2 - pass explicit path to credential file location
If you prefer to pass an explicit path to your json credentials file (useful if you cannot access env variables, say in a test enviroment), then you can use this code:
```csharp
var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
{
	JsonAuthFile = CredentialPath,
	...
});
```


## Interface Operations

We can be explicit when using the messenger instances with `ISendMessages` (send operations only), `IReactiveMessenger` (streaming messages using observables) and `IMessenger` (simple callbacks for receiving messages).
Read the full interface here: [IMessenger.cs](https://github.com/rmccabe24/Cloud.Core/blob/master/src/Cloud.Core/IMessenger.cs)

If you just want to only send messages, you would consume `ISendMessages`.

If you want to send and receive with a simple call back interface use the `IMessenger`.

If instead you want a reactive IObservable that you can subscribe to, use the `IReactiveMessenger`.  The streaming functionality is very useful for messengers that don't offer push functionality out of the box, as with Azure Service Bus that works by polling the message queue.

You can call `Abandon`, `Complete` and `Error` on the `IMessage` interface.

- All messages arrive with a perpetual Lock applied in the form of a renewal timer on the message.

- `Abandon` will abandon the message, returning it to the original queue.

- `Complete` will actually perform the "Read" on the message taking it from the queue.

- `Error` will move the message to the error queue (dead letter queue), used during critical processing errors where there may be problems with 
validation, business rules, incorrect data, etc.

### How to send a message

The simplest way to do it is by consuming IMessenger and calling `Send` for a single message and `SendBatch` to send a batch of messages (the package handles sending the list of items in batches for you):

```csharp
IMessenger msn = new PubSubMessenger(configuration);

msn.Send(new TestMessage{ Name = "Some Name", Stuff = "Some Stuff"  });

msn.SendBatch(new List<TestMessage> {  
  new TestMessage{ Name = "Some Name 1", Stuff = "Some Stuff 1"  },
  new TestMessage{ Name = "Some Name 2", Stuff = "Some Stuff 2"  },
  new TestMessage{ Name = "Some Name 3", Stuff = "Some Stuff 2"  }
});
```

## Send and receive messages

The messenger implementation allows for generic [POCOs](https://en.wikipedia.org/wiki/Plain_Old_CLR_Object) class types to be used to define the type of messages being sent and received.  Using a generic allows the object types, already used within the calling app, to be reused as message contents.

Here's an example of a simple class that we'll send:

```csharp
public class TestMessage : IMessage
{
    public string Name { get; set; }
    public string Stuff { get; set; }
}
```

Note: max allowed messages in a single batch is 1000.  So if you request a larger batch size it will be limited internally for you.






### Entity Disabled Exception
If you try to send messages to a queue or topic (or topic subscription) that is disabled, the `Send` code will throw an Cloud.Core.Messenger.EntityDisabledException that you can specifically catch and handle in your code. 

The custom error allows code to stay generic and not need to look for service bus specific errors.  Littering code with platform specific api code.  Extend these custom errors as new scenarios arise.

**Example**
```csharp
try
{
    // Example of sending a single message to the configured queue.
    await messenger.Send<string[]>(new[] { "test" });
}
catch (Core.Exceptions.EntityDisabledException edEx)
{
    // exception occured - handle - potentially put app to sleep and try again shortly...
}
```

### How to constantly receive messages using observables
You can subscribe to new messages using the observable provided by the IReactiveMessenger interface.

```csharp
IReactiveMessenger msn = new ServiceBusMessenger(config);
            
msn.StartReceive<TestMessage>().Subscribe(
  async receivedMsg => {
  
      // Write processing code here...

      // after processing, complete the message.
      await msn.Complete(receivedMsg);
  },
  failedEx => {  
      // an exception has occurred.
  });
```

### How to constantly receive messages using callbacks
You can pass callback's into the Receive method as provided IMessenger interface.

```csharp
IMessenger msn = new ServiceBusMessenger(config);
            
msn.Receive<TestMessage>(
  async receivedMsg => {
  
      // Write processing code here...

      // after processing, complete the message.
      await msn.Complete(receivedMsg);
  },
  failedEx => {  
      // an exception has occurred.
  });
```

### How to receive one message at a time
You can stay in control of messages arrive by using the receive one method as shown below.  This is for scenarios where messages are not to be constantly streamed.

```csharp
IMessenger msn = new ServiceBusMessenger(config);
            
var singleMessage = msn.ReceiveOne<TestMessage>();

// Process message...

await msn.Complete(singleMessage);
```


## Full working example

```csharp
var messenger = new ServiceBusMessenger(new Core.Messaging.AzureServiceBus.Config.MsiConfig
{
    InstanceName = namespaceHelper.MessagingServiceInstanceName,
    SubscriptionId = Settings.SubscriptionId,
    TenantId = Settings.TenantId,
                    
    // Information about the queue or topic that will be listened to.
    Receiver = new ReceiverSetup
    {
        // use topic instead of queue (in this case topic - topic is set by default).
        UseTopic = true,
        // automatically create if its not there
        CreateEntityIfNotExists = true, 
        // name of topic
        EntityName = "RobertsTestTopic", 
        // subscription to listen on
        EntitySubscriptionName = "RobertsTestSubscription",  
        // (if creating) specify filter to apply to subscription.  This example filters on messages tagged version 1.0.
        EntityFilter = new KeyValuePair<string, string>("RobertFilterExample", "Version = '1.0'"),
        // How ofter to renew the lock on the message.  Uses the Max Allowed Locktime from service bus to make sure its 
        // always less than the max allowed value if a larger value is specified.  Actually renews on 80% of this time.
        LockRenewalTimeInSeconds = 60,
        // How often to check for messages.  0.05 is the default BUT can be slowed down (or sped up) as required.
        PollFrequencyInSeconds = 0.05,
        // Old versions of service bus defaulted to string content.  Now it's default is stream.  There's a bit of overhead
        // when dealing with the string types, so this is defaulted to false unless you know you want extra compatibility when
        // setting up your listener (like listening off a subscription an old function sends to).
        SupportStringBodyType = false
    },
    // Information about the queue or topic to send messages to.
    Sender = new SenderSetup
    {
        // use queue instead of topic.
        UseTopic = false,
        // Create the queue if it doesnt already exist.
        CreateEntityIfNotExists = true,
        // Name of queue to send to.
        EntityName = "RobertsTestQueue",
        // Property "Version" for the message.  Always set, can be used for filtering.
        MessageVersion = 2.1
    }
});

var manager = messenger.EntityManager;

// The `ToString()` method shows full information about the service bus instance, sender and receiver.
Console.WriteLine(manager.ToString());

// The entity manager also contains useful methods for getting message count and entity usage percentage.
var senderMessageCount = await manager.GetSenderMessageCount();
var senderEntityUsagePercent = await manager.GetSenderEntityUsagePercentage();
var receiverEntityUsagePercent = await manager.GetReceiverEntityUsagePercentage();
var receiverMessageCount = await manager.GetReceiverMessageCount();
var isReceiverEntityDisabled = await manager.IsReceiverEntityDisabled();
var isSenderEntityDisabled = await manager.IsSenderEntityDisabled();
                
// Example of sending a single message to the configured queue.
await messenger.Send<string>("test");

// Receive one message from the configured topic (runs synchronously).
var messageItem = messenger.ReceiveOne<string>();

// Setup a subscribable to constantly listent for new messages arrive.
// Application needs to stay alive to keep this running.  Use AppHost `RunAndBlock()` method to support this.
// Alternative for testing is `Console.ReadLine()`.
messenger.StartReceive<string>(10).Subscribe(async message =>
{
    // Process messages here....

    // Complete the message when finished.
    await messenger.Abandon(message);   // return message to queue/topic without completing (will be picked up again).
    await messenger.Error(message);     // deadletter message.
    await messenger.Complete(message);  // complete and remove the message.
});

// When we no longer need to listen for messages, we can cancel using this:
messenger.CancelReceive<string>();

// When finished with the messenger, you can dispose the instance.
messenger.Dispose();
```

## Back-off Mechanism 

This API comes with an optional backoff-mechanism.  It only works when the wrapper is both receiving messages from one queue/topic and then sending on to another.  

When enabled (use the `EnableAutobackOff = true` config option), the code will monitor the topic its sending messages to and if it becomes greater than 90%, it will "Backoff" by stopping the receiver temporarily for 2 minutes.  After the 2 minutes are up, it will check the sender entity to see if it's fallen below the threshold of 90% and if so, will resume the receive and begin sending messages again.

This will apply back pressure - i.e. the receiver entity will fill up then, so be careful when using.  Works better if the previous component (sending messages to the receiver entity also has back-off enabled).


## *NOTE* Ones to watch out for...
### Body Type
There are three body types supported when sending messages - Stream, String and WCF.  By default ALL messages are sent and (expected to be) received 
with body type *Stream".  Content as a stream is more performant, so by default String support *IS NOT* enabled.  To enable this, when instantiating
the AzureServiceBus client and passing in the configuration, make sure to set the *SupportStringBodyType* property to _true_.

This property has been retrospectively added for backwards support with existing Topics and messages.

### Topic Setup - Don't enable partitioning
Preimum and should have "EnablePartitioning" set to false.  This can only be set when the topic is being created
so make sure that is the case when the infrastructure deployment scripts are being setup.


## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard and can be therefore be referenced from a .net Core or .net Framework application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on 
Windows (or Linux using Mono).
 
## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, it can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a public NuGet feed.  To consume this on your local development machine, please add the following feed to your feed sources in Visual Studio:
https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=feed&feed=Cloud.Core
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<a href="https://dev.azure.com/cloudcoreproject/CloudCore" target="_blank">
<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
</a>
