package backchat.zeromq

import akka.actor._
import Messages._
import java.util.Locale.ENGLISH

case class DeviceConfig(context: Context, name: String, serverAddress: String, pollTimeout: Long = -1)

trait PubSubProxy extends ZeroMQDevicePart { self: ZeroMQDevice ⇒

  def addressToProxy: String
  val pubsubProxyAddress = "inproc://" + deviceName + "-publisher.inproc"

  protected val subscriber = context.socket(Sub)
  protected val pubsubProxy = context.socket(Pub)

  protected def handleIncoming(zmsg: ZMessage) {
    zmsg(pubsubProxy)
  }

  abstract override def dispose() {
    pubsubProxy.close()
    subscriber.close()
    super.dispose()
  }

  abstract override def init() {
    subscriber.connect(addressToProxy)
    subscriber.subscribe("".getBytes(ZMessage.defaultCharset))
    pubsubProxy.bind(pubsubProxyAddress)
    poller += (subscriber -> (handleIncoming _))
    super.init()
  }
}

trait Puller extends ZeroMQDevicePart { self: ZeroMQDevice ⇒
  def pusherAddress: String
  def callbackId: String

  protected val puller = context socket Pull

  abstract override def send(zmsg: ZMessage) {
    Actor.registry.actorsFor(callbackId).headOption foreach { _ ! ApplicationEvent(zmsg.body) }
  }

  abstract override def dispose() {
    puller.close()
    super.dispose()
  }

  abstract override def init() {
    super.init()
    puller.connect(pusherAddress)
    poller += (puller -> (send _))
  }
}

trait LoadBalancedPusher extends ZeroMQDevicePart { self: ZeroMQDevice ⇒
  def pusherAddress: String
  val pusherProxyAddress = "inproc://" + deviceName + ".inproc"

  protected val pusher = context socket Push
  protected val pusherProxy = context socket Dealer

  abstract override def send(zmsg: ZMessage) {
    zmsg(pusher)
  }

  abstract override def dispose() {
    pusher.close()
    pusherProxy.close()
    super.dispose()
  }

  abstract override def init() {
    super.init()
    pusher.bind(pusherAddress)
    pusherProxy.bind(pusherProxyAddress)
    poller += (pusherProxy -> (send _))
  }
}

trait PubSubSubscriber extends ZeroMQDevicePart with ZmqBroker { self: ZeroMQDevice with ZmqBroker ⇒
  abstract override protected def inboundHandler(zmsg: ZMessage) {
    logger.trace("Subscriber [%s] got message for handler '%s': %s" format (deviceName, handler, zmsg))
    if (zmsg.messageType.toLowerCase(ENGLISH) == "pubsub" && zmsg.sender.toLowerCase(ENGLISH) == "publish") {
      handler foreach { h ⇒
        //        val msg = ZMessage("", newCcId, "pubsub", "", zmsg.target, zmsg.body)
        logger.trace("forwarding pubsub message %s" format zmsg)
        Actor.registry.actorFor(h) foreach { _ ! ProtocolMessage(zmsg) }
      }
    } else {
      super.inboundHandler(zmsg)
    }
  }

}
trait ServerPubSubSubscriber extends ZeroMQDevicePart with ZeroMQBroker { self: ZeroMQDevice with ZeroMQBroker ⇒

  abstract override def send(zmsg: ZMessage) {
    if (zmsg.messageType.toLowerCase(ENGLISH) == "pubsub") {
      zmsg.sender.toLowerCase(ENGLISH) match {
        case "subscribe" | "unsubscribe" ⇒ {
          logger.trace("[SOCKET] %sing to: %s" format (zmsg.sender.substring(0, zmsg.sender.length - 1), zmsg.target))
          zmsg(router)
        }
        case _ ⇒ super.send(zmsg)
      }
    } else {
      super.send(zmsg)
    }
  }
}

trait PubSubPublisher extends ZeroMQDevicePart with ZmqBroker { self: ZeroMQDevice with ZmqBroker ⇒

  abstract override protected def inboundHandler(zmsg: ZMessage) {
    logger.trace("Pubsub publisher got: %s" format zmsg)
    zmsg.messageType.toLowerCase(ENGLISH) match {
      case "pubsub" ⇒ {
        val routes = Subscription(zmsg.addresses)
        zmsg.sender.toLowerCase(ENGLISH) match {
          case "subscribe" if zmsg.target.isBlank ⇒ sendToHandler {
            SubscribeAll -> routes
          }
          case "subscribe" ⇒ sendToHandler {
            TopicSubscription(zmsg.target, routes)
          }
          case "unsubscribe" if zmsg.target.isBlank ⇒ sendToHandler {
            UnsubscribeAll -> routes
          }
          case "unsubscribe" ⇒ sendToHandler {
            TopicUnsubscription(zmsg.target, routes)
          }
          case _ ⇒ super.inboundHandler(zmsg)
        }
      }
      case _ ⇒ super.inboundHandler(zmsg)
    }
  }

  private def sendToHandler(msg: Any) {
    handler foreach {
      Actor.registry.actorFor(_) foreach { _ ! msg }
    }
  }

}

trait ServerPubSubPublisher extends PubSubPublisher { self: ZeroMQDevice with ZeroMQBroker ⇒
  abstract override def send(zmsg: ZMessage) {
    if (zmsg.messageType == "pubsub" && zmsg.sender == "publish") {
      logger.trace("publishing message: %s" format zmsg)
      zmsg(router)
    } else {
      super.send(zmsg)
    }
  }
}

trait ZmqBroker {
  protected var handler: Option[Uuid] = None
  protected def inboundHandler(zmsg: ZMessage)
}
trait ZeroMQBroker extends ZmqBroker { self: ZeroMQDevice ⇒
  protected val router = context.socket(Router)
}

case class ClientPing(clientId: Array[Byte])
trait PingPongResponder extends ZeroMQBroker { this: ZeroMQDevice with ZeroMQBroker ⇒

  abstract override protected def inboundHandler(zmsg: ZMessage) {
    logger.trace("inbound in pingpong responder got: %s" format zmsg)
    if (zmsg.messageType == "system") {
      zmsg.body.toUpperCase(ENGLISH) match {
        case "PING" ⇒ {
          zmsg.addresses.lastOption foreach { id ⇒
            Actor.registry.actorFor[PingPongObserver] foreach { _ ! ClientPing(id) }
          }
          zmsg.sender = deviceName + "-endpoint"
          zmsg.body = "PONG"
          zmsg(router)
        }
        case _ ⇒ super.inboundHandler(zmsg)
      }
    } else {
      super.inboundHandler(zmsg)
    }
  }

}

trait ActorBridgeCreation extends ZeroMQDevicePart { self: ZeroMQDevice ⇒

  protected def newActor: Actor
  protected def createActorBridge() = {
    val a = Actor.actorOf(newActor)
    ZeroMQ.supervisor startLink a
    a
  }

  abstract override def init() {
    super.init()
    createActorBridge()
  }
}

trait ServerActorBridge extends ZeroMQDevicePart with ZeroMQBroker { self: ZeroMQDevice ⇒
  def routerAddress: String
  val actorBridgeAddress: String = "inproc://" + deviceName + ".inproc"
  protected val actorBridge = context.socket(Dealer)
  protected var activeRequests = Map[String, ZMessage]()

  protected def inboundHandler(zmsg: ZMessage) {
    logger.trace("Router [%s] got message: %s" format (deviceName, zmsg))
    handler foreach { h ⇒
      if (zmsg.messageType.toLowerCase(ENGLISH) == "requestreply") activeRequests += zmsg.ccid -> zmsg
      Actor.registry.actorFor(h) foreach { _ ! ProtocolMessage(zmsg) }
    }
  }

  abstract override def dispose() {
    logger.trace("Stopping ServerActorBridge %s" format deviceName)
    actorBridge.close()
    router.close()
    super.dispose()
  }

  abstract override def init() {
    logger.trace("Starting ServerActorBridge %s" format deviceName)
    super.init()
    router.setIdentity((deviceName + "-endpoint").getBytes(ZMessage.defaultCharset))
    router.bind(routerAddress)
    logger.trace("bound router to %s" format routerAddress)
    actorBridge.setIdentity(deviceName.getBytes(ZMessage.defaultCharset))
    actorBridge.bind(actorBridgeAddress)
    logger.trace("bound bridge to %s" format actorBridgeAddress)
    poller += (router -> (inboundHandler _))
    poller += (actorBridge -> (send _))
  }

  protected def setHandler(h: String) {
    logger.trace("setting handler to %s" format h)
    handler = Some(new Uuid(h))
  }

  protected def clearHandler(h: String) {
    logger.trace("clearing handler %s" format h)
    val sndrId = new Uuid(h)
    if (handler.forall(_ == sndrId)) handler = None // only reset the handler if it's the same uuid
  }

  abstract override def send(zmsg: ZMessage) {
    logger.trace("[%s] handling message: %s" format (deviceName, zmsg))
    zmsg.messageType.toLowerCase(ENGLISH) match {
      case "system" if (List("READY", "STOPPING").contains(zmsg.body.toUpperCase(ENGLISH))) ⇒ {
        logger.trace("[%s] handling system message: %s" format (deviceName, zmsg))
        zmsg.body.toUpperCase(ENGLISH) match {
          case "READY"    ⇒ setHandler(zmsg.unwrap())
          case "STOPPING" ⇒ clearHandler(zmsg.unwrap())
        }
      }
      case "requestreply" ⇒ {
        logger.trace("[%s] handling requestreply message: %s" format (deviceName, zmsg))
        //zmsg.unwrap()
        zmsg.addresses = activeRequests(zmsg.ccid).addresses
        activeRequests -= zmsg.ccid
        logger.trace("[%s] sending requestreply reply: %s" format (deviceName, zmsg))
        zmsg(router)
      }
      case _ ⇒ {
        logger.trace("[%s] forwarding message to next in chain" format deviceName)
        super.send(zmsg)
      }
    }
  }

}

class BackchatZeroMqDevice(config: DeviceConfig) extends ZeroMQDevice {
  protected val context = config.context

  val id = Symbol("backchat-∅MQ-" + config.name.toString)
  val deviceName = config.name

}

