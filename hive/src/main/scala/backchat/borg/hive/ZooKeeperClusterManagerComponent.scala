package backchat.borg
package hive

import akka.actor._
import java.io.IOException
import org.apache.zookeeper._
import akka.util.Switch
import scalaz._
import Scalaz._
import Zero._
import scala.util.control.Exception._

trait ZooKeeperClusterManagerComponent extends ClusterManagerComponent {
  this: ClusterNotificationManagerComponent ⇒

  sealed trait ZooKeeperMessage
  object ZooKeeperMessages {
    case object Connected extends ZooKeeperMessage
    case object Disconnected extends ZooKeeperMessage
    case object Expired extends ZooKeeperMessage
    case class NodeChildrenChanged(path: String) extends ZooKeeperMessage
  }

  type ZooKeeperFactory = (String, Duration, Watcher) ⇒ ZooKeeper
  class ZooKeeperClusterManager(connectString: String, sessionTimeout: Duration, serviceName: String)(implicit zooKeeperFactory: ZooKeeperFactory) extends Actor with ClusterManagerHelper with Logging {
    private val ServiceNode = "/" + serviceName
    private val AvailabilityNode = ServiceNode + "/available"
    private val MembershipNode = ServiceNode + "/members"

    private val Nodes = List(ServiceNode, AvailabilityNode, MembershipNode)

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

      case NodeChildrenChanged(path) ⇒ if (path == AvailabilityNode) {
        handleAvailabilityChanged()
      } else if (path == MembershipNode) {
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

        val availableSet = zk.getChildren(AvailabilityNode, true).foldLeft(Set[Int]()) { (set, i) ⇒ set + i.toInt }
        if (availableSet.size == 0) {
          currentNodes.foreach { case (id, _) ⇒ makeNodeUnavailable(id) }
        } else {
          val (available, unavailable) = currentNodes partition { case (id, _) ⇒ availableSet.contains(id) }
          available foreach { case (id, _) ⇒ makeNodeAvailable(id) }
          unavailable foreach { case (id, _) ⇒ makeNodeUnavailable(id) }
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
        val path = "%s/%d".format(MembershipNode, node.id)

        if (zk.exists(path, false).isNotNull) {
          new InvalidNodeException("A node with id %d already exists".format(node.id)).some
        } else {

          try {
            zk.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

            currentNodes += (node.id -> node)
            clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)

            None
          } catch {
            case ex: KeeperException if ex.code() == KeeperException.Code.NODEEXISTS ⇒
              new InvalidNodeException("A node with id %d already exists".format(node.id)).some
          }
        }
      }
    }

    private def handleRemoveNode(nodeId: Int) {
      logger.debug("Handling a RemoveNode(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a RemoveNode message", "deleting node") { zk ⇒
        val path = "%s/%d".format(MembershipNode, nodeId)
        deletePath(zk, path)
        currentNodes -= nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def handleMarkNodeAvailable(nodeId: Int) {
      logger.debug("Handling a MarkNodeAvailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeAvailable message", "marking node available") { zk ⇒
        val path = "%s/%d".format(AvailabilityNode, nodeId)

        if (zk.exists(path, false).isNull) {
          ignoringIf(_.code == KeeperException.Code.NODEEXISTS) {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          }
        }

        makeNodeAvailable(nodeId)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def deletePath(zk: ZooKeeper, path: String) {
      if (zk.exists(path, false).isNotNull) {
        ignoringIf(_.code == KeeperException.Code.NONODE) {
          zk.delete(path, -1)
        }
      }
    }

    private def handleMarkNodeUnavailable(nodeId: Int) {
      logger.debug("Handling a MarkNodeUnavailable(%d) message".format(nodeId))

      doIfConnectedWithZooKeeperWithResponse("a MarkNodeUnavailable message", "marking node unavailable") { zk ⇒
        val path = "%s/%d".format(AvailabilityNode, nodeId)
        deletePath(zk, path)
        makeNodeUnavailable(nodeId)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        None
      }
    }

    private def ignoringIf[T](predicate: KeeperException ⇒ Boolean)(body: ⇒ T)(implicit z: Zero[T]): T = {
      val catcher: Catcher[T] = {
        case ex: KeeperException if predicate(ex) ⇒ z.zero
      }
      catching(catcher) apply body
    }

    private def handleShutdown() {
      logger debug "Handling a Shutdown message"

      (handling(classOf[Exception])
        by (logger.error("Exception when closing connection to ZooKeeper", _))
        andFinally {
          logger info "ZooKeeperClusterManager shut down"
          self.stop()
        }) {
          watcher.shutdown()
          zooKeeper.foreach(_.close())
        }
    }

    private def startZooKeeper() {
      zooKeeper = (handling(classOf[Exception]) by {
        case e: IOException ⇒ logger error ("Unable to connect to ZooKeeper", e); None
        case e: Exception   ⇒ logger error ("Exception while connecting to ZooKeeper", e); None
      }) {
        watcher = new ClusterWatcher(self)
        val zk = zooKeeperFactory(connectString, sessionTimeout, watcher)
        logger.debug("Connected to ZooKeeper")
        zk.some
      }
    }

    private def verifyZooKeeperStructure(zk: ZooKeeper) {
      logger.debug("Verifying ZooKeeper structure...")

      Nodes foreach { path ⇒
        ignoringIf(_.code() == KeeperException.Code.NODEEXISTS) {
          logger.debug("Ensuring %s exists".format(path))
          if (zk.exists(path, false).isNull) {
            logger.debug("%s doesn't exist, creating".format(path))
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
          }; () // return Unit and save a cheerleader
        }
      }
    }

    private def lookupCurrentNodes(zk: ZooKeeper) {
      import scala.collection.JavaConversions._

      val members = zk.getChildren(MembershipNode, true)
      val available = zk.getChildren(AvailabilityNode, true)

      currentNodes.clear()

      members.foreach { member ⇒
        val id = member.toInt
        currentNodes += (id -> Node(id, zk.getData("%s/%s".format(MembershipNode, member), false, null), available.contains(member)))
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
      zooKeeper some { zk ⇒
        ((handling(classOf[KeeperException]) by (logger.error("ZooKeeper threw an exception", _))) or
          (handling(classOf[Exception]) by (logger.error("Unhandled exception while working with ZooKeeper", _)))){ block(zk) }
      } none {
        logger.error(
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
          val response =
            ((handling(classOf[KeeperException]) by (new ClusterException("Error while %s".format(exceptionDescription), _).some)) or
              (handling(classOf[Exception]) by (new ClusterException("Unexpected exception while  %s".format(exceptionDescription), _).some))) {
                block(zk)
              }

          self tryReply ClusterManagerResponse(response)
        }
      } else {
        self tryReply ClusterManagerResponse(Some(new ClusterDisconnectedException("Error while %s, cluster is disconnected".format(exceptionDescription))))
      }
    }
  }

  protected implicit def defaultZooKeeperFactory(connectString: String, sessionTimeout: Duration, watcher: Watcher): ZooKeeper =
    new ZooKeeper(connectString, sessionTimeout.getMillis.toInt, watcher)

  class ClusterWatcher(zooKeeperManager: ActorRef) extends Watcher {
    private val switch = new Switch(true)

    def process(event: WatchedEvent) {
      import org.apache.zookeeper.Watcher.Event.{ EventType, KeeperState }
      import ZooKeeperMessages._
      switch ifOn {
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
    }

    def shutdown(): Unit = switch.switchOff
  }
}
