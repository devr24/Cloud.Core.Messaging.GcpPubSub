# **Cloud.Core.Messaging.GcpPubSub**  
[![Build status](https://dev.azure.com/cloudcoreproject/CloudCore/_apis/build/status/Cloud.Core%20Packages/Cloud.Core.Messenger.GcpPubSub_Package)](https://dev.azure.com/cloudcoreproject/CloudCore/_build/latest?definitionId=35) ![Code Coverage](https://cloud1core.blob.core.windows.net/codecoveragebadges/Cloud.Core.Messaging.GcpPubSub-LineCoverage.png) [![Cloud.Core.Messaging.GcpPubSub package in Cloud.Core feed in Azure Artifacts](https://feeds.dev.azure.com/cloudcoreproject/dfc5e3d0-a562-46fe-8070-7901ac8e64a0/_apis/public/Packaging/Feeds/8949198b-5c74-42af-9d30-e8c462acada6/Packages/b65f3009-b2dc-47a9-ab21-732b5c6e8475/Badge)](https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=package&feed=8949198b-5c74-42af-9d30-e8c462acada6&package=b65f3009-b2dc-47a9-ab21-732b5c6e8475&preferRelease=true)

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

### How to stream messages using observables
You can subscribe to new messages using the observable provided by the IReactiveMessenger interface.

```csharp
IReactiveMessenger msn = new PubSubMessenger(config);
            
msn.StartReceive<TestMessage>().Subscribe(
  async receivedMsg => {
  
      // Write processing code here...

  },
  failedEx => {  
      // an exception has occurred.
  });
```

_Note: messages are automatically acknowledged here due to restrictions in the PubSub SDK._

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

_Note: messages are automatically acknowledged here._

### How to receive one message at a time
You can stay in control of messages arriving by using the receive one method as shown below.  This is for scenarios where messages are not to be constantly streamed (Push) and are gathered using Pull.

```csharp
IMessenger msn = new PubSubMessenger(config);
            
var singleMessage = msn.ReceiveOne<TestMessage>();

// Process message...

await msn.Complete(singleMessage);
```

### How to receive one message entity at a time
If you need access to message properties directly, you can use `ReceiveOneEntity` which gives you access to the typed content and message properties. 

```csharp
IMessenger msn = new PubSubMessenger(config);
            
var singleEntity = msn.ReceiveOneEntity<TestMessage>();
// OR you can specify the topic inline using the concrete class:
var msg = ((PubSubMessenger)msg).ReceiveOne<TestMessage>("AnySubscription");

// Process message...
var props = singleEntity.Props;
var body = singleEntity.Body

await msn.Complete(singleEntity);
// OR
await msg.Complete(singleEntity.Body);
```

### How to receive a batch of messages
You can receive a batch of messages in one single synchronous Pull.

```csharp
IMessenger msn = new PubSubMessenger(config);
            
var messages = msn.ReceiveBatch<TestMessage>(500);
// OR you can specify the topic inline using the concrete class:
var msgs = ((PubSubMessenger)msg).ReceiveBatch<TestMessage>("AnySubscription", 50);

// Process messages...

await msn.CompleteAll(messages);
```

### How to return a message to the topic
You can abandon message processing by:

```csharp
IMessenger msn = new PubSubMessenger(config);
            
var message = msn.ReceiveOne<TestMessage>();

// Process messages...

await msn.Abandon(message);
```

### How to dead-letter a message
You can dead-letter a message (put on error topic) by:

```csharp
IMessenger msn = new PubSubMessenger(config);
            
var message = msn.ReceiveOne<TestMessage>();

// Process messages...

await msn.Error(message); // move to dead-letter topic
```

### Entity Send/Recieve Authorisation
To carry out any reading/publishing of messages, the follwoing GCP PubSub permissions are required:

- Pub/Sub Subscriber - for reading messages in a stream
- Pub/Sub Viewer - for receiving messages one by one

## Managing Topics

### Create Topic Entity

Using create entity will carry out the following:
1) Create the topic with the name provider.
2) Create a default subscription (if you don't specify one). This will be `{topicName}_default` - the idea with this is that no messages sent will be missed and not picked up by a topic.
3) Creates a dead-letter topic and associates the subscription created with that topic.
4) Creates a default subscription for the dead-letter topic - so no messages which are dead-lettered are missed.

The code to carry this out would be:

```csharp
IMessenger messenger = new PubSubMessenger(new PubSubConfig { ProjectId = _config["GcpProjectId"] });
var manager = messenger.EntityManager;

await manager.CreateEntity("MyTopic");

// Only create full entity if any parts don't already exist.
await manager.CreateEntityIfNotExists("MyTopic"); 

// Create topic, dead-letter topic, subscription named, and deadletter subscription if required. 
await manager.CreateEntity("MyTopic", "MyTopicDeadletter, "MySubscription", "MySubscriptionDeadletter"); 
```

The manager has explicit methds for topic (only) and subscription (only) creation.  Example code:

```csharp
IMessenger messenger = new PubSubMessenger(new PubSubConfig { ProjectId = _config["GcpProjectId"] });
PubSubManager manager = messenger.EntityManager;

await manager.CreateTopic("MyTopic");
await manager.CreateSubscription("MySubscription");
```

### Entity Exists
You can check to see if a topic entity exists by using this code:

```csharp
IMessenger messenger = new PubSubMessenger(new PubSubConfig { ProjectId = _config["GcpProjectId"] });
var manager = messenger.EntityManager;

var exists = await manager.EntityExists("MyTopic");

if (!exists)
{
   ...
}
```

### Delete Entity
You can also delete the topic entity with this code. Worth noting that associated subscriptions WILL be deleted.  If you do not want this behaviour then use the explicit `DeleteTopic` method also shown below:

```csharp
IMessenger messenger = new PubSubMessenger(new PubSubConfig { ProjectId = _config["GcpProjectId"] });
var manager = messenger.EntityManager;

await manager.DeleteEntity("MyTopic"); // Topic plus subscriptions deleted.
await ((PubSubManager)manager).DeleteTopic("MyTopic"); // Only topic.
await ((PubSubManager)manager).DeleteSubscription("MyTopic_default"); // Only subscription.
```

### Using Message Filtering

At some point we may only want a subscription to pickup specific messages with a particular attribute.  Here's an example ofsubscription message filtering:

```csharp
var messenger = new PubSubMessenger(new PubSubJsonAuthConfig()
{
    JsonAuthFile = _fixture.CredentialPath,
    ProjectId = _fixture.ProjectId
});
var manager = messenger.EntityManager as PubSubManager;

// Act
// Create the filter topic for testing.
manager.CreateTopic(_fixture.MessageFilterTopic, null, "defaultsub").GetAwaiter().GetResult();
manager.CreateSubscription(_fixture.MessageFilterTopic, "filteredsub", "attributes:pickme").GetAwaiter().GetResult();

// Send two messages, one that wont be picked up by the filter subscription and the other that
// will.  The result is two messages to the defaultsub, one to filteredsub
messenger.Send(_fixture.MessageFilterTopic, "test").GetAwaiter().GetResult();
messenger.Send(_fixture.MessageFilterTopic, "testfilter", new KeyValuePair<string, object>[]
{
    new KeyValuePair<string, object>("pickme", "please") 
}).GetAwaiter().GetResult();

// Receive from both subscriptions.
var nonFilteredMessages = messenger.ReceiveBatch<string>("defaultsub", 100).GetAwaiter().GetResult();
var filteredMessages = messenger.ReceiveBatch<string>("filteredsub", 100).GetAwaiter().GetResult();
```

Full filtering documtation on GCP Pub/Sub can be found here: [https://cloud.google.com/pubsub/docs/filtering](https://cloud.google.com/pubsub/docs/filtering)

## Entity Manager Authorisation
To carry out any create or delete entity (topics/subscriptions), the following GCP PubSub permissions are required:

- Pub/Sub Admin

## Using Service Collection Extensions

For ease of use, there's a convenient way to add to the service collection IOC container during a typical Startup of an application.  Here's how adding the messenger without the extension would look:

```csharp
public class Startup
{
    public void ConfigureServices(IConfiguration config, ILogger logger, IServiceCollection services)
    {
        // Setup application settings (options).
        var settings = config.BindBaseSection<AppSettings>();
        services.AddSingleton(settings);

		services.AddSingleton(new PubSubMessenger(new PubSubConfig {
            ProjectId = "projectid",
        });
		// OR to use one of the interfaces
		services.AddSingleton<IReactiveMessenger>(new PubSubMessenger(new PubSubConfig {
            ProjectId = "projectid",
        });
		services.AddSingleton<IMessenger>(new PubSubMessenger(new PubSubConfig {
            ProjectId = "projectid",
        });
		
		// Because there is more than one, we need to manually wire up the named instance factory for consumption with the specific type.
		services.AddSingleton<NamedInstanceFactory<IMessenger>>();
		
		... 
    }
}
```

And here's how it can be replaced using the extensions:

```csharp
public class Startup
{
    public void ConfigureServices(IConfiguration config, ILogger logger, IServiceCollection services)
    {
        // Setup application settings (options).
        var settings = config.BindBaseSection<AppSettings>();
        services.AddSingleton(settings);

		services.AddPubSubSingleton(new PubSubConfig {
            ProjectId = "projectid" 
		});
		// OR to use one of the interfaces
		services.AddPubSubSingleton<IReactiveMessenger>(new PubSubConfig {
            ProjectId = "projectid",
        });
		services.AddPubSubSingleton<IMessenger>(new PubSubConfig {
            ProjectId = "projectid",
        });
		
		... 
    }
}
```

It doesn't look like a massive difference but the main thing is you get the named instance factory setup for free with the call.

## Full working example

```csharp       
IReactiveMessenger messenger = new PubSubMessenger(new PubSubConfig
{
    ProjectId = _config["GcpProjectId"],
    ReceiverConfig = new ReceiverConfig()
     {
	 EntityName = "sourceTopic",
	 CreateEntityIfNotExists = true // create the topic, default subscription and dead-letter topic/subscription
     },
     Sender = new SenderConfig()
     {
	 EntityName = "targetTopic",
	 CreateEntityIfNotExists = true // create the topic and default subscription
     }
});
       
// Example of sending a single message to the configured queue.
await messenger.Send<string>("test");

// Receive one message from the configured topic (runs synchronously).
var messageItem = messenger.ReceiveOne<string>();

// Setup a subscribable to constantly stream new messages.
// Application needs to stay alive to keep this running. Use AppHost `RunAndBlock()` method to support this.
messenger.StartReceive<string>(10).Subscribe(async message =>
{
    // Process messages here....

    // Messages auto complete when dealt with.
});

// When we no longer need to listen for messages, we can cancel using this:
messenger.CancelReceive<string>();

// When finished with the messenger, you can dispose the instance.
messenger.Dispose();
```

## Test Coverage
A threshold will be added to this package to ensure the test coverage is above 80% for branches, functions and lines.  If it's not above the required threshold 
(threshold that will be implemented on ALL of the core repositories to gurantee a satisfactory level of testing), then the build will fail.

## Compatibility
This package has has been written in .net Standard 2.1, therefore be only referenced from a .net Core application. The advantage of utilising from a .net Core application, 
is that it can be deployed and run on a number of host operating systems, such as Windows, Linux or OSX.  Unlike referencing from the a .net Framework application, which can only run on Windows (or Linux using Mono).

## Setup
This package is built using .net Standard 2.1 and requires the .net Core 3.1 SDK, which can be downloaded here: 
https://www.microsoft.com/net/download/dotnet-core/

IDE of Visual Studio or Visual Studio Code, can be downloaded here:
https://visualstudio.microsoft.com/downloads/

## How to access this package
All of the Cloud.Core.* packages are published to a public NuGet feed.  To consume this on your local development machine, the .nuget file is included but you can also add the following feed to your feed sources in Visual Studio:
https://dev.azure.com/cloudcoreproject/CloudCore/_packaging?_a=feed&feed=Cloud.Core
 
For help setting up, follow this article: https://docs.microsoft.com/en-us/vsts/package/nuget/consume?view=vsts


<a href="https://dev.azure.com/cloudcoreproject/CloudCore" target="_blank">
<img src="https://cloud1core.blob.core.windows.net/icons/cloud_core_small.PNG" />
</a>
