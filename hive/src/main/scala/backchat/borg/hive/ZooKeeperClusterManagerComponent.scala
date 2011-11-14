package backchat.borg
package hive

import akka.actor._
import java.io.IOException
import org.apache.zookeeper._

trait ZooKeeperClusterManagerComponent extends ClusterManagerComponent {
  this: ClusterNotificationManagerComponent ⇒

  sealed trait ZooKeeperMessage
  object ZooKeeperMessages {
    case object Connected extends ZooKeeperMessage
    case object Disconnected extends ZooKeeperMessage
    case object Expired extends ZooKeeperMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperMessage
  }

  class ZooKeeperClusterManager(connectString: String, sessionTimeout: Duration, serviceName: String)(implicit zooKeeperFactory: (String, Duration, Watcher) ⇒ ZooKeeper) extends Actor with ClusterManagerHelper with Logging {
    private val SERVICE_NODE = "/" + serviceName
    private val AVAILABILITY_NODE = SERVICE_NODE + "/available"
    private val MEMBERSHIP_NODE = SERVICE_NODE + "/members"

    private val currentNodes = scala.collection.mutable.Map[Int, Node]()
    private var zooKeeper: Option[ZooKeeper] = None
    private var watcher: ClusterWatcher = _
    private var connected = false

    override def preStart() {
      logger.debug("Connecting to ZooKeeper...")
      startZooKeeper()
    }

    import ZooKeeperMessages._
    import ClusterManagerMessages._

    protected def receive = {
      case Connected    ⇒ handleConnected()

      case Disconnected ⇒ handleDisconnected()

      case Expired      ⇒ handleExpired()

      case NodeChildrenChanged(path) ⇒ if (path == AVAILABILITY_NODE) {
        handleAvailabilityChanged()
      } else if (path == MEMBERSHIP_NODE) {
        handleMembershipChanged()
      } else {
        logger.error("Received a notification for a path that shouldn't be monitored: %s".format(path))
      }

      case AddNode(node)               ⇒ handleAddNode(node)

      case RemoveNode(nodeId)          ⇒ handleRemoveNode(nodeId)

      case MarkNodeAvailable(nodeId)   ⇒ handleMarkNodeAvailable(nodeId)

      case MarkNodeUnavailable(nodeId) ⇒ handleMarkNodeUnavailable(nodeId)

      case Shutdown                    ⇒ handleShutdown()

      case m                           ⇒ logger.error("Received unknown message: %s".format(m))

    }

    private def handleConnected() {
      logger.debug("Handling a Connected message")

      if (connected) {
        logger.error("Received a Connected message when already connected")
      } else {
        doWithZooKeeper("a Connected message") { zk ⇒
          verifyZooKeeperStructure(zk)
          lookupCurrentNodes(zk)
          connected = true
          clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)
        }
      }
    }

    private def handleDisconnected() {
      logger.debug("Handling a Disconnected message")

      doIfConnected("a Disconnected message") {
        connected = false
        currentNodes.clear()
        clusterNotificationManager ! ClusterNotificationMessages.Disconnected
      }
    }

    private def handleExpired() {
      logger.debug("Handling an Expired message")

      logger.error("Connection to ZooKeeper expired, reconnecting...")
      connected = false
      currentNodes.clear()
      watcher.shutdown()
      startZooKeeper()
    }

    private def handleAvailabilityChanged() {
      logger.debug("Handling an availability changed event")

      doIfConnectedWithZooKeeper("an availability changed event") { zk ⇒
        import scala.collection.JavaConversions._

        val availableSet = zk.getChildren(AVAILABILITY_NODE, true).foldLeft(Set[Int]()) { (set, i) ⇒ set + i.toInt }
        if (availableSet.size == 0) {
          currentNodes.foreach { case (id, _) ⇒ makeNodeUnavailable(id) }
        } else {
          val (available, unavailable) = currentNodes.partition { case (id, _) ⇒ availableSet.contains(id) }
          available.foreach { case (id, _) ⇒ makeNodeAvailable(id) }
          unavailable.foreach { case (id, _) ⇒ makeNodeUnavailable(id) }
        }

        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleMembershipChanged() {
      logger.debug("Handling a membership changed event")

      doIfConnectedWithZooKeeper("a membership changed event") { zk ⇒
        lookupCurrentNodes(zk)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
      }
    }

    private def handleAddNode(node: Node) {
      logger.debug("Handling an AddNode(%s) message".format(node))

      doIfConnectedWithZooKeeperWithResponse("an AddNode message", "adding node") { zk ⇒
        val path = "%s/%d".format(MEMBERSHIP_NODE, node.id)

        if (zk.exists(path, false) != null) {
          Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))
        } else {
          try {
            zk.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

            currentNodes += (node.id -> node)
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)

            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS ⇒ Some(new InvalidNodeException("A node with id %d already exists".format(node.id)))
          }
        }
      }
    }

    private def handleRemoveNode(nodeId: Int) {
      logger.debug("Handling a RemoveNode(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a RemoveNode message", "deleting node") { zk ⇒
        val path = "%s/%d".format(MEMBERSHIP_NODE, nodeId)

        if (zk.exists(path, false) != null) {
          try {
            zk.delete(path, -1)
          } catch {
            case ex: KeeperException if ex.code() == KeeperException.Code.NONODE ⇒ // do nothing
          }
        }

        currentNodes -= nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleMarkNodeAvailable(nodeId: Int) {
      logger.debug("Handling a MarkNodeAvailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeAvailable message", "marking node available") { zk ⇒
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)

        if (zk.exists(path, false) == null) {
          try {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS ⇒ // do nothing
          }
        }

        makeNodeAvailable(nodeId)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleMarkNodeUnavailable(nodeId: Int) {
      logger.debug("Handling a MarkNodeUnavailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeUnavailable message", "marking node unavailable") { zk ⇒
        val path = "%s/%d".format(AVAILABILITY_NODE, nodeId)

        if (zk.exists(path, false) != null) {
          try {
            zk.delete(path, -1)
            None
          } catch {
            case ex: KeeperException if ex.code == KeeperException.Code.NONODE ⇒ // do nothing
          }
        }

        makeNodeUnavailable(nodeId)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleShutdown() {
      logger.debug("Handling a Shutdown message")

      try {
        watcher.shutdown()
        zooKeeper.foreach(_.close())
      } catch {
        case ex: Exception ⇒ logger.error("Exception when closing connection to ZooKeeper", ex)
      }

      logger.debug("ZooKeeperClusterManager shut down")
      self.stop()
    }

    private def startZooKeeper() {
      zooKeeper = try {
        watcher = new ClusterWatcher(self)
        val zk = Some(zooKeeperFactory(connectString, sessionTimeout, watcher))
        logger.debug("Connected to ZooKeeper")
        zk
      } catch {
        case ex: IOException ⇒
          logger.error("Unable to connect to ZooKeeper", ex)
          None

        case ex: Exception ⇒
          logger.error("Exception while connecting to ZooKeeper", ex)
          None
      }
    }

    private def verifyZooKeeperStructure(zk: ZooKeeper) {
      logger.debug("Verifying ZooKeeper structure...")

      List(SERVICE_NODE, AVAILABILITY_NODE, MEMBERSHIP_NODE).foreach { path ⇒
        try {
          logger.debug("Ensuring %s exists".format(path))
          if (zk.exists(path, false) == null) {
            logger.debug("%s doesn't exist, creating".format(path))
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }
        } catch {
          case ex: KeeperException if ex.code == KeeperException.Code.NODEEXISTS ⇒ // do nothing
        }
      }
    }

    private def lookupCurrentNodes(zk: ZooKeeper) {
      import scala.collection.JavaConversions._

      val members = zk.getChildren(MEMBERSHIP_NODE, true)
      val available = zk.getChildren(AVAILABILITY_NODE, true)

      currentNodes.clear()

      members.foreach { member ⇒
        val id = member.toInt
        currentNodes += (id -> Node(id, zk.getData("%s/%s".format(MEMBERSHIP_NODE, member), false, null), available.contains(member)))
      }
    }

    private def makeNodeAvailable(nodeId: Int) {
      currentNodes.get(nodeId).foreach { n ⇒ if (!n.available) currentNodes.update(n.id, n.copy(available = true)) }
    }

    private def makeNodeUnavailable(nodeId: Int) {
      currentNodes.get(nodeId).foreach { n ⇒ if (n.available) currentNodes.update(n.id, n.copy(available = false)) }
    }

    private def doIfConnected(what: String)(block: ⇒ Unit) {
      if (connected) block else logger.error("Received %s when not connected".format(what))
    }

    private def doWithZooKeeper(what: String)(block: ZooKeeper ⇒ Unit) {
      zooKeeper match {
        case Some(zk) ⇒
          try {
            block(zk)
          } catch {
            case ex: KeeperException ⇒ logger.error("ZooKeeper threw an exception", ex)
            case ex: Exception       ⇒ logger.error("Unhandled exception while working with ZooKeeper", ex)
          }

        case None ⇒ logger.error(
          "Received %s when ZooKeeper is None, this should never happen. Please report a bug including the stack trace provided.".format(what),
          new Exception)
      }
    }

    private def doIfConnectedWithZooKeeper(what: String)(block: ZooKeeper ⇒ Unit) {
      doIfConnected(what) {
        doWithZooKeeper(what)(block)
      }
    }

    private def doIfConnectedWithZooKeeperWithResponse(what: String, exceptionDescription: String)(block: ZooKeeper ⇒ Option[ClusterException]) {
      import ClusterManagerMessages.ClusterManagerResponse

      if (connected) {
        doWithZooKeeper(what) { zk ⇒
          val response = try {
            block(zk)
          } catch {
            case ex: KeeperException ⇒ Some(new ClusterException("Error while %s".format(exceptionDescription), ex))
            case ex: Exception       ⇒ Some(new ClusterException("Unexpected exception while %s".format(exceptionDescription), ex))
          }

          self tryReply ClusterManagerResponse(response)
        }
      } else {
        self tryReply ClusterManagerResponse(Some(new ClusterDisconnectedException("Error while %s, cluster is disconnected".format(exceptionDescription))))
      }
    }
  }

  protected implicit def defaultZooKeeperFactory(connectString: String, sessionTimeout: Duration, watcher: Watcher) =
    new ZooKeeper(connectString, sessionTimeout.getMillis.toInt, watcher)

  class ClusterWatcher(zooKeeperManager: ActorRef) extends Watcher {
    @volatile
    private var shutdownSwitch = false

    def process(event: WatchedEvent) {
      import org.apache.zookeeper.Watcher.Event.{ EventType, KeeperState }
      import ZooKeeperMessages._

      if (shutdownSwitch) return

      event.getType match {
        case EventType.None ⇒
          event.getState match {
            case KeeperState.SyncConnected ⇒ zooKeeperManager ! Connected
            case KeeperState.Disconnected  ⇒ zooKeeperManager ! Disconnected
            case KeeperState.Expired       ⇒ zooKeeperManager ! Expired
          }

        case EventType.NodeChildrenChanged ⇒ zooKeeperManager ! NodeChildrenChanged(event.getPath)
      }
    }

    def shutdown(): Unit = shutdownSwitch = true
  }
}
