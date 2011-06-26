# A transport for akka with zeromq.

This is pretty much a work in progress.
At this moment we have reliable client to server broker. 

## How to run the tests?
1. Build/install the jzmq library
2. Add -Djava.library.path=/path/to/jzmq to your xsbt startup script  
3. run the test suite:  
```
xsbt 'test-only com.mojolly.backchat.zeromq.tests.ZeroMqSuite'  
```

example xsbt script:  

```
#!/bin/sh
java -Xms512m -Xmx1024m -XX:MaxPermSize=256m -XX:+CMSClassUnloadingEnabled  -Djava.library.path=/usr/local/lib -Dfile.encoding='UTF-8' -jar `dirname $0`/xsbt-launch.jar "$@"
```


## How is it put together?

This library starts with the idea that you want to create custom 0mq devices.  
And different applications have different needs so this library (ab)uses stackable traits so that a 0mq device becomes composable.  
You can add pubsub by just mixing in a trait.

This library does invert the idea that 0mq has about pubsub. This pubsub only works on TCP but there is publisher side filtering 
instead of subscriber side filtering.

For example to create the simplest implementation of a client/server paradigm 

```scala

/* the server device */
ZeroMQ startDevice {
  new BackchatZeroMqDevice(serverConfig) with ServerActorBridge {
    val routerAddress = serverConfig.serverAddress
  }
}
val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge).start()


/* the client device */
ZeroMQ startDevice {
  new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker {
    val outboundAddress = clientConfig.serverAddress
    override def init() {
      super.init()
      clientLatch.open()
    }
  }
}
val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge).start()

/* Getting a result from the server */
clientBridge !! Request(serverActorName)

```

If you want to add reliable pubsub

```scala
val serverBroker = ZeroMQ startDevice {
  new BackchatZeroMqDevice(serverConfig) with ServerActorBridge with ServerPubSubPublisher {
    val routerAddress = serverConfig.serverAddress
  }
}
val serverBridge = actorOf(new ZeroMqBridge(context, serverConfig.name) with ServerBridge with PublisherBridge).start()

val clientBroker = ZeroMQ startDevice {
  new BackchatZeroMqDevice(clientConfig) with ClientActorBridge with ClientBroker with ClientPubSubSubscriber {
    val outboundAddress = clientConfig.serverAddress
  }
}
val clientBridge = actorOf(new ZeroMqBridge(context, clientConfig.name) with ClientBridge with SubscriberBridge).start()
```
