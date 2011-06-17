package com.mojolly.backchat
package zeromq

import queue.ApplicationEvent
import akka.actor._
import org.zeromq.ZMQ.Poller
import Messages._
import collection.immutable.SortedSet

trait ZmqClient {

  def enqueue(queueName: String, appEvent: ApplicationEvent)
}
trait ZmqActorClient extends ZmqClient {
  def request(target: String, appEvent: ApplicationEvent, requestTimeout: Duration)(implicit someSelf: Option[ActorRef]): Option[Any]
  def bridgeActor: Option[ActorRef]
}
trait ZeroMqSubscribing { self: ZmqClient ⇒
  def subscribe(topic: String, subscriber: ActorRef)
}
trait ZeroMqPublishing { self: ZmqClient ⇒
  def publish(topic: String, appEvent: ApplicationEvent)
}
trait ZeroMqClient extends ZmqClient {
  def request(target: String, appEvent: ApplicationEvent)(onReply: ApplicationEvent ⇒ Any)
  def dispose()
  //  def publish(topic: String, appEvent: ApplicationEvent)
}
trait OnReady {
  protected def onReady(handler: String)
  protected def onUnavailable(handler: String)
}

trait ClientZmqBroker extends ZmqBroker { self: ZeroMQDevice with ZmqBroker ⇒
  protected val outbound: Socket
  protected def sendToServer(zmsg: ZMessage)
}

trait ClientPubSubSubscriber extends ZeroMQDevicePart with ClientZmqBroker { self: ZeroMQDevice with ClientZmqBroker ⇒

  abstract override def send(zmsg: ZMessage) {
    if (zmsg.messageType.toLowerCase(ENGLISH) == "pubsub") {
      zmsg.sender.toLowerCase(ENGLISH) match {
        case "subscribe" | "unsubscribe" ⇒ {
          trace("[SOCKET] %sing to: %s", zmsg.sender.substring(0, zmsg.sender.length - 1), zmsg.target)
          sendToServer(zmsg)
        }
        case _ ⇒ super.send(zmsg)
      }
    } else {
      super.send(zmsg)
    }
  }
}
trait ClientPubSubPublisher extends PubSubPublisher { self: ZeroMQDevice with ClientZmqBroker ⇒

  abstract override def send(zmsg: ZMessage) {
    if (zmsg.messageType == "pubsub" && zmsg.sender == "publish") {
      trace("publishing message: %s", zmsg)
      zmsg(outbound)
    } else {
      super.send(zmsg)
    }
  }
}

trait ClientActorBridge extends ZeroMQDevicePart with OnReady { self: ZeroMQDevice with ZmqBroker ⇒
  val actorBridgeAddress: String = "inproc://" + deviceName + ".inproc"
  protected val actorBridge = context.socket(Router)
  //  protected var handler: Option[Uuid] = None

  abstract override def dispose() {
    trace("Stopping ClientActorBridge %s", deviceName)
    actorBridge.close()
    super.dispose()
  }

  abstract override def init() {
    trace("Starting ClientActorBridge %s", deviceName)
    super.init()
    actorBridge.bind(actorBridgeAddress)
    trace("bound bridge to %s", actorBridgeAddress)
    poller += (actorBridge -> (send _))
  }

  protected def setHandler(h: String) {
    trace("setting handler to %s", h)
    handler = Some(new Uuid(h))
    onReady(h)
  }

  protected def onReady(h: String) { // allow stackable traits

  }

  protected def onUnavailable(h: String) {

  }

  protected def clearHandler(h: String) {
    trace("clearing handler %s", h)
    val sndrId = new Uuid(h)
    if (handler.forall(_ == sndrId)) handler = None // only reset the handler if it's the same uuid
    onUnavailable(h)
  }

  abstract override def send(zmsg: ZMessage) {
    trace("client actor bridge handling messag: %s", zmsg)
    zmsg.messageType.toLowerCase(ENGLISH) match {
      case "system" if (List("READY", "STOPPING").contains(zmsg.body.toUpperCase(ENGLISH))) ⇒ {
        trace("[%s] handling system message: %s", deviceName, zmsg)
        zmsg.body.toUpperCase(ENGLISH) match {
          case "READY" ⇒ setHandler(zmsg.unwrap())
          case "STOPPING" ⇒ {
            clearHandler(zmsg.unwrap())
          }
        }
      }
      case _ ⇒ {
        trace("[%s] forwarding message to next in chain", deviceName)
        super.send(zmsg)
      }
    }
  }
}

object ReliableClientBroker {
  class AvailableServers(servers: Map[String, AvailableServer]) extends Map[String, AvailableServer] {
    def this(srvrs: (String, AvailableServer)*) = this(Map(srvrs: _*))
    private val ss = servers

    def iterator = ss.iterator

    def +[B1 >: AvailableServer](kv: (String, B1)) = new AvailableServers(ss + kv.asInstanceOf[(String, AvailableServer)])

    def -(key: String) = new AvailableServers(ss - key)

    def get(key: String) = ss.get(key)

  }
  case class AvailableServer(endpoint: String, ttl: Duration = 2.seconds)
  class ActiveServers(servers: Set[ActiveServer]) extends Set[ActiveServer] {
    def this(srvrs: ActiveServer*) = this(Set(srvrs: _*))
    private val ss = servers
    def iterator = ss.iterator

    def -(elem: ActiveServer) = new ActiveServers(ss.filterNot(_.server == elem.server).toSeq: _*)

    def +(elem: ActiveServer) = {
      val sss = ss.filterNot(_.server.endpoint == elem.server.endpoint)
      new ActiveServers(sss + elem)
    }
    implicit def ordering = Ordering.fromLessThan[ActiveServer] { (left, right) ⇒
      if (left.nextPing == right.nextPing) left.responseTime < right.responseTime
      else left.nextPing < right.nextPing
    }

    def pickServer = SortedSet(ss.toSeq: _*)(ordering) headOption
    def withoutExpired = new ActiveServers(ss.filter(_.expires >= DateTime.now))
    def thatNeedAPing = ss.filter(_.nextPing <= DateTime.now)
    val smallestPingTimeout =
      SortedSet(ss.toSeq: _*)(Ordering.fromLessThan(_.server.ttl.millis < _.server.ttl.millis)).headOption.map(_.server.ttl.millis) getOrElse 2000L

    def contains(elem: ActiveServer) = ss.contains(elem)
  }
  case class ActiveServer(server: AvailableServer, endpoint: String, responseTime: Duration, nextPing: DateTime = MIN_DATE, expires: DateTime = MIN_DATE)

  case class ActiveRequest(req: ZMessage, server: String, created: DateTime, expires: DateTime, retries: Int = 0)
}

trait ReliableClientBroker extends ZeroMQDevicePart with ClientZmqBroker with OnReady { self: ZeroMQDevice with ClientActorBridge ⇒

  import ReliableClientBroker._
  protected val outbound = context.socket(Router)

  protected val maxRetries = 3
  protected val requestTimeout = 5.seconds
  protected var availableServers: AvailableServers = new AvailableServers()
  protected var activeServers: ActiveServers = new ActiveServers()
  protected var activeRequests = Map[String, ActiveRequest]()
  protected var registeredInPoller = false

  protected def isActive = handler.isDefined
  protected def isConnected = activeServers.size > 0

  protected def slideTimeouts(endpoint: String) {
    trace("sliding timeouts for: %s", endpoint)
    availableServers.get(endpoint) foreach { serv ⇒
      activeServers.find(_.server == serv) foreach { as ⇒
        activeServers -= as
        val exp = new Duration(serv.ttl.millis * 3)
        val newer = ActiveServer(serv, endpoint, as.responseTime, DateTime.now + serv.ttl, DateTime.now + exp)
        trace("adding newer to active servers: %s", newer)
        activeServers += newer
      }
    }
  }

  protected def inboundHandler(zmsg: ZMessage) {
    trace("Router [%s] got message: %s", deviceName, zmsg)
    try {
      whenActive {
        zmsg match {
          case _ if (zmsg.messageType.toLowerCase(ENGLISH) == "system" && zmsg.body.toUpperCase(ENGLISH) == "PONG") ⇒ {
            // activate server and/or update ttl expiration
            val respTime = activeRequests.get(zmsg.ccid) map { ar ⇒
              new Interval(ar.created, DateTime.now).toDuration
            } getOrElse (new Duration(Long.MaxValue))
            availableServers.get(zmsg.sender) foreach { serv ⇒
              val exp = new Duration(serv.ttl.millis * 3)
              activeServers += ActiveServer(serv, zmsg.sender, respTime, DateTime.now + serv.ttl, DateTime.now + exp)
              // yay we're connected. We've got a response let's reissue all the active requests.
              activeRequests.get(zmsg.ccid) foreach { _ ⇒ activeRequests -= zmsg.ccid }
              onConnected(serv.endpoint)
              trace("active servers: %s", activeServers)
            }
          }
          case _ ⇒ {
            sendToBridge(zmsg)
          }
        }
      }
    } finally {
      activeRequests.get(zmsg.ccid) foreach { _ ⇒ activeRequests -= zmsg.ccid }
    }
  }

  protected def onConnected(endpoint: String) {
    retryRequests(endpoint)
  }

  protected def sendToBridge(zmsg: ZMessage) {
    handler map { h ⇒
      Actor.registry.actorFor(h) foreach { _ ! ProtocolMessage(zmsg) }
      h
    } getOrElse {
      zmsg(actorBridge)
    }
    serverNameForCcid(zmsg.ccid) foreach { slideTimeouts _ }
  }

  protected def serverNameForCcid(ccid: String) = {
    activeRequests.get(ccid).map(_.server)
  }

  abstract override def dispose() {
    trace("Stopping ReliableClientBroker %s", deviceName)
    outbound.close()
    activeServers = new ActiveServers()
    activeRequests = Map[String, ActiveRequest]()
    super.dispose()
  }

  abstract override protected def onUnavailable(handler: String) {
    // remove from active servers
  }

  abstract override protected def onReady(handler: String) {
    // send ping message to all available servers
    if (availableServers.isEmpty) sendToBridge(Error(new Uuid, "SERVER_UNAVAILABLE").toZMessage)
    availableServers foreach {
      case (endp, serv) ⇒ {
        trace("trying to connect to: (%s, %s)", endp, serv)
        outbound.connect(serv.endpoint)
        if (!registeredInPoller) {
          poller += (outbound -> (inboundHandler _))
          registeredInPoller = true
        }
        trackRequest(endp, Ping.toZMessage)
      }
    }
  }

  protected def trackRequest(endp: String, msg: ZMessage) {
    trace("tracking request for server '%s': %s", endp, msg)
    if (msg.messageType == "requestreply" || msg.body == "PING") {
      val ar = if (msg.body == "PING") ActiveRequest(msg, endp, DateTime.now, new DateTime(Long.MaxValue))
      else ActiveRequest(msg, endp, DateTime.now, requestTimeout.from(DateTime.now))
      trace("registering active request: %s", ar)
      activeRequests += msg.ccid -> ar
    }
    val toSend = msg.wrap(endp)
    trace("sending: %s", toSend)
    toSend(outbound)
  }

  abstract override def send(zmsg: ZMessage) {
    trace("[%s] handling message: %s", deviceName, zmsg)
    zmsg.messageType.toLowerCase(ENGLISH) match {
      case "requestreply" ⇒ {
        trace("[%s] handling %s message in ReliableClientBroker: %s", deviceName, zmsg.messageType, zmsg)
        sendToServer(zmsg)
      }
      case "fireforget" ⇒ {
        trace("[%s] handling %s message: %s", deviceName, zmsg.messageType, zmsg)
        sendToServer(zmsg)
      }
      case "system" if (List("READY", "STOPPING").contains(zmsg.body.toUpperCase(ENGLISH))) ⇒ {
        trace("[%s] handling system message: %s", deviceName, zmsg)
        zmsg.body.toUpperCase(ENGLISH) match {
          case "READY" ⇒ {
            zmsg.unwrap()
            setHandler(zmsg.unwrap())
          }
          case "STOPPING" ⇒ {
            zmsg.unwrap()
            clearHandler(zmsg.unwrap())
          }
        }
      }
      case _ ⇒ {
        trace("[%s] forwarding message to next in chain", deviceName)
        super.send(zmsg)
      }
    }
  }

  protected def sendToServer(zmsg: ZMessage) {
    trace("[%s] Entering send to server: %s\nservers: %s", deviceName, zmsg, activeServers)
    activeServers.pickServer map { as ⇒
      trackRequest(as.endpoint, zmsg)
      as
    } getOrElse {
      trace("sending unavailable from sendToServer")
      sendToBridge(Error(new Uuid(zmsg.ccid), "SERVER_UNAVAILABLE").toZMessage)
    }
  }

  protected def sendPings {
    activeServers.thatNeedAPing.filterNot(as ⇒ activeRequests.exists(_._2.server == as.endpoint)) foreach { as ⇒
      trackRequest(as.endpoint, Ping.toZMessage)
    }
  }

  protected def whenActive(action: ⇒ Unit) {
    if (isActive) action
  }

  protected def expireRequests {
    if (isConnected) {
      val expired = activeRequests.filterNot(_._2.expires > DateTime.now)
      trace("The expired requests: %s", expired)
      val fullyExpired = expired.filter(_._2.retries >= maxRetries)
      trace("the fully expired requests: %s", fullyExpired)
      fullyExpired foreach { kv ⇒ sendToBridge(Error(new Uuid(kv._1), "TIMEOUT").toZMessage) }
      val toRetry = expired.filterNot(kv ⇒ fullyExpired.contains(kv._1)) map {
        case (k, v) ⇒ k -> v.copy(retries = v.retries + 1, expires = requestTimeout.from(DateTime.now))
      }
      trace("the requests to retry: %s", toRetry)
      activeRequests = activeRequests.filterNot(kv ⇒ expired.contains(kv._1)) ++ (toRetry map { r ⇒ r._1 -> retryRequest(r._2) })
    }
  }

  protected def retryRequests(server: String) {
    val retried = activeRequests.filter(_._2.server == server) map {
      case (ccid, ar) ⇒
        ar.req.wrap(ar.server)(outbound)
        ccid -> ar.copy(retries = (ar.retries + 1), expires = requestTimeout.from(DateTime.now))
    }
    trace("Retried the requests: %s", retried)
    activeRequests ++= retried
  }

  protected def retryRequest(ar: ActiveRequest) = {
    trace("retrying request: %s", ar)
    new ActiveServers(activeServers.filterNot(_.endpoint == ar.server).toSeq: _*).pickServer map { server ⇒
      ar.req.wrap(server.endpoint)(outbound)
      ar.copy(server = server.endpoint)
    } getOrElse {
      ar.req.wrap(ar.server)(outbound)
      ar
    }
  }

  protected def expireServers {
    if (isConnected) {
      trace("expiring servers")
      val stillActive = activeServers.withoutExpired
      trace("the active servers: %s", stillActive)
      val expired = activeServers.filterNot(stillActive.contains)
      trace("the expired servers: %s", expired)
      activeServers = stillActive
      val expiredRequests = activeRequests.filter(as ⇒ expired.exists(_.endpoint == as._2.server) && as._2.req.body != "PING")
      trace("the expired requests: %s", expiredRequests)
      activeRequests = activeRequests.filterKeys(!expiredRequests.keySet.contains(_))
      trace("The remaining active requests: %s", activeRequests)
      if (!isConnected) {
        trace("sending unavailable from expire servers")
        sendToBridge(Error(new Uuid(), "SERVER_UNAVAILABLE").toZMessage)
      } else {
        if (expiredRequests.keySet.size > 0) trace("sending unavailable to the active request dispatchers")
        expiredRequests.keySet foreach { ccid ⇒ sendToBridge(Error(new Uuid(ccid), "SERVER_UNAVAILABLE").toZMessage) }
      }
    }
  }

  abstract override def execute() = {
    whenActive {
      expireRequests
      expireServers
    }
    sendPings
    poller.poll(activeServers.smallestPingTimeout)
    keepRunning
  }
}

trait ClientBroker extends ZeroMQDevicePart with ClientZmqBroker { self: ZeroMQDevice with ClientActorBridge ⇒
  val outboundAddress: String

  protected val outbound = context.socket(Dealer)

  protected def inboundHandler(zmsg: ZMessage) {
    trace("Router [%s] got message: %s", deviceName, zmsg)
    if (handler.isDefined) {
      handler foreach { h ⇒
        Actor.registry.actorFor(h) foreach { _ ! ProtocolMessage(zmsg) }
      }
    } else {
      zmsg(actorBridge)
    }
  }

  abstract override def dispose() {
    trace("Stopping ClientBroker %s", deviceName)
    outbound.close()
    super.dispose()
  }

  abstract override def init() {
    trace("Starting ClientBroker %s", deviceName)
    super.init()
    outbound.setIdentity(deviceName.getBytes(Utf8))
    outbound.connect(outboundAddress)
    trace("connected outbound to %s", outboundAddress)
    poller += (outbound -> (inboundHandler _))
  }

  protected def sendToServer(zmsg: ZMessage) { zmsg(outbound) }

  abstract override def send(zmsg: ZMessage) {
    trace("[%s] handling message: %s", deviceName, zmsg)
    zmsg.messageType.toLowerCase(ENGLISH) match {
      case "requestreply" | "fireforget" ⇒ {
        trace("[%s] handling %s message: %s", deviceName, zmsg.messageType, zmsg)
        sendToServer(zmsg)
      }
      case _ ⇒ {
        trace("[%s] forwarding message to next in chain", deviceName)
        super.send(zmsg)
      }
    }
  }
}
class BackchatZeroMqClient(val id: String, context: Context, deviceName: String, receiveTimeout: Duration = 15.seconds) extends ZeroMqClient {

  protected val client: Socket = context socket Dealer
  client.setIdentity(id.getBytes(ZMessage.defaultCharset))
  client.connect("inproc://" + deviceName + ".inproc")
  client.setLinger(0)

  protected val poller = context.poller(1)
  poller.register(client, Poller.POLLIN)

  def enqueue(target: String, appEvent: ApplicationEvent) {
    ZMessage("", newCcId, "fireforget", "", target, appEvent.toJson)(client)
  }

  def request(target: String, appEvent: ApplicationEvent)(onReply: (ApplicationEvent) ⇒ Any) {
    ZMessage("", newCcId, "requestreply", id, target, appEvent.toJson)(client)
    poller.poll(receiveTimeout.millis * 1000)
    if (poller.pollin(0)) {
      val msg = ZMessage(client)
      if (msg.messageType == "system" && msg.sender == "ERROR") {
        msg.body match {
          case "SERVER_UNAVAILABLE" ⇒ {
            throw new ServerUnavailableException
          }
          case "TIMEOUT" ⇒ {
            throw new RequestTimeoutException("The request to " + target + " with data: " + appEvent.toJson + " timed out.")
          }
        }
      } else {
        onReply(ApplicationEvent(msg.body))
      }
    } else {
      throw new RequestTimeoutException("The request to " + target + " with data: " + appEvent.toJson + " timed out.")
    }
  }

  def dispose() {
    client.close()
  }
}
class RequestTimeoutException(msg: String = "Request timed out.") extends Exception(msg)
class ServerUnavailableException extends Exception("No server could be reached.")
