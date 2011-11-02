package backchat.borg
package cluster

import akka.actor._
import java.util.concurrent.atomic.AtomicLong

trait ClusterNotificationManagerComponent {
  def clusterNotificationManager: ActorRef
  sealed trait ClusterNotificationMessage

  object ClusterNotificationMessages {
    case class AddListener(listener: ActorRef) extends ClusterNotificationMessage
    case class AddedListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case class RemoveListener(key: ClusterListenerKey) extends ClusterNotificationMessage
    case class Connected(nodes: Set[Node]) extends ClusterNotificationMessage
    case object Disconnected extends ClusterNotificationMessage
    case class NodesChanged(nodes: Set[Node]) extends ClusterNotificationMessage
    case object Shutdown extends ClusterNotificationMessage
    case object GetCurrentNodes extends ClusterNotificationMessage
    case class CurrentNodes(nodes: Set[Node]) extends ClusterNotificationMessage
  }

  class ClusterNotificationManager(callback: Option[ActorRef]) extends Actor with Logging {
    def this() = this(None)

    import ClusterNotificationMessages._

    private var currentNodes = Set.empty[Node]
    private val listenerId = new AtomicLong(0)
    private var listeners = Map[ClusterListenerKey, ActorRef]()
    private var _connected = false

    protected def receive = {
      case AddListener(listener) ⇒ {
        addListener(listener)
        callback foreach { _ ! AddListener(listener) }
      }
      case RemoveListener(key) ⇒ {
        removeListener(key)
        callback foreach { _ ! RemoveListener(key) }
      }
      case Connected(nodes) ⇒ {
        connected(nodes)
        callback foreach { _ ! Connected(nodes) }
      }
      case Disconnected ⇒ {
        disconnected()
        callback foreach { _ ! Disconnected }
      }
      case NodesChanged(nodes) ⇒ {
        nodesChanged(nodes)
        callback foreach { _ ! NodesChanged(nodes) }
      }
      case Shutdown ⇒ {
        shutdown()
        callback foreach { _ ! Shutdown }
      }
      case GetCurrentNodes ⇒ {
        self tryReply CurrentNodes(currentNodes)
        callback foreach { _ ! GetCurrentNodes }
      }
    }

    def addListener(listener: ActorRef) {
      logger debug "Handling AddListener(%s)".format(listener.id)
      val key = ClusterListenerKey(listenerId.incrementAndGet())
      listeners += key -> listener
      if (_connected) listener ! ClusterEvents.Connected(availableNodes)
      self tryReply AddedListener(key)
    }

    def removeListener(key: ClusterListenerKey) {
      logger.debug("Handling RemoveListener(%s) message".format(key))
      listeners.get(key) match {
        case Some(a) ⇒
          a ! 'quit
          listeners -= key

        case None ⇒ logger.info("Attempt to remove an unknown listener with key: %s".format(key))
      }
    }

    def connected(nodes: Set[Node]) {
      logger.debug("Handling Connected(%s) message".format(nodes))

      if (_connected) {
        logger.warn("Received a Connected event when already connected")
      } else {
        _connected = true
        currentNodes = nodes

        notifyListeners(ClusterEvents.Connected(availableNodes))
      }
    }

    def disconnected() {
      logger.debug("Handling Disconnected message")

      if (_connected) {
        _connected = false
        currentNodes = Set()

        notifyListeners(ClusterEvents.Disconnected)
      } else {
        logger.warn("Received a Disconnected event when disconnected")
      }
    }

    def nodesChanged(nodes: Set[Node]) {
      logger.debug("Handling NodesChanged(%s) message".format(nodes))

      if (_connected) {
        currentNodes = nodes

        notifyListeners(ClusterEvents.NodesChanged(availableNodes))
      } else {
        logger.warn("Received a NodesChanged event when disconnected")
      }

    }

    def shutdown() {
      logger.debug("Handling Shutdown message")

      notifyListeners(ClusterEvents.Shutdown)
      notifyListeners('quit)

      currentNodes = Set()
      logger.debug("ClusterNotificationManager shut down")
      if (self.supervisor.isDefined) self.supervisor.foreach(_ ! UnlinkAndStop(self)) else self.stop()
    }

    private def notifyListeners(event: Any) = listeners.values.filter(_.isRunning).foreach(_ ! event)

    private def availableNodes = currentNodes.filter(_.available == true)

  }
}
