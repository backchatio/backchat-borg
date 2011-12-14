package backchat.borg

import akka.actor._
import Actor._
import scalaz._
import Scalaz._
import collection.JavaConversions._
import com.twitter.zookeeper.{ ZooKeeperClient, ZooKeeperClientConfig }
import akka.routing.{ Deafen, Listen, Listeners }
import net.liftweb.json._
import JsonDSL._
import java.util.concurrent.ConcurrentHashMap
import collection.mutable.ConcurrentMap

trait TrackerRegistryListener {

  def condition(node: TrackerRegistry.TrackerNode) = true
  def ifConditionValid(node: TrackerRegistry.TrackerNode)(thunk: TrackerRegistry.TrackerNode ⇒ Any) {
    if (condition(node)) thunk(node)
  }

  def onTrackerAdded(node: TrackerRegistry.TrackerNode) {

  }
  def onTrackerUpdated(node: TrackerRegistry.TrackerNode, previous: TrackerRegistry.TrackerNode) {

  }
  def onTrackerRemoved(node: TrackerRegistry.TrackerNode) {

  }
}

object TrackerRegistry {

  object TrackerNode {
    def apply(bytes: Array[Byte]): TrackerNode = {
      TrackerNode(Protos.TrackerNode parseFrom bytes)
    }

    def apply(proto: Protos.TrackerNode): TrackerNode = {
      TrackerNode(
        proto.getId,
        proto.getSource,
        Option(proto.getKindList) map (Vector(_: _*)) getOrElse Vector.empty,
        proto.getNodeId,
        Option(proto.getServicesList) map (Vector(_: _*)) getOrElse Vector.empty,
        ServiceType(proto.getProvides))
    }
  }

  case class TrackerNode(
      id: String,
      source: String,
      kinds: Seq[String],
      nodeId: Long,
      services: Seq[String] = Vector.empty,
      provides: ServiceType.EnumVal = ServiceType.Tracker) extends Subject[String] {

    type ProtoBufMessage = Protos.TrackerNode

    override def toJValue: JValue = {
      ("id" -> id) ~
        ("source" -> source) ~
        ("kinds" -> kinds) ~
        ("nodeId" -> nodeId) ~
        ("provides" -> provides.name)
    }

    def toProtobuf = {
      (Protos.TrackerNode.newBuilder
        setId id
        setSource source
        addAllKind kinds
        setNodeId nodeId
        addAllServices services
        setProvides provides.pbType).build()
    }
  }
  object Messages {

    sealed trait TrackerRegistryMessage
    case class GetTracker(id: String) extends TrackerRegistryMessage
    case class SetTracker(node: TrackerNode) extends TrackerRegistryMessage
    case class RemoveTracker(node: TrackerNode) extends TrackerRegistryMessage
    case class TrackerAdded(node: TrackerNode) extends TrackerRegistryMessage
    case class TrackerUpdated(node: TrackerNode, previous: TrackerNode) extends TrackerRegistryMessage
    case class TrackerRemoved(node: TrackerNode) extends TrackerRegistryMessage
  }

  def start(config: ZooKeeperClientConfig, supervisor: Option[ActorRef] = None) {
    val zkClient = new ZooKeeperClient(config)
    zkClient.connect()
    val context = TrackerRegistryConfig(zkClient)
    val reg = supervisor map { sup ⇒
      val a = actorOf(new TrackerRegistryActor(context))
      sup startLink a
      a
    } getOrElse {
      actorOf(new TrackerRegistryActor(context)).start()
    }
    reg
  }

  def addListener(list: TrackerRegistryListener) = {
    val wrapped = wrapInActor(list)
    registry.actorFor[TrackerRegistryActor] foreach { _ ! Listen(wrapped) }
    wrapped
  }

  def removeListener(list: ActorRef) = {
    registry.actorFor[TrackerRegistryActor] foreach { _ ! Deafen(list) }
  }

  private def wrapInActor(list: TrackerRegistryListener) = {
    actorOf(new Actor {
      protected def receive = {
        case Messages.TrackerAdded(node)             ⇒ list.ifConditionValid(node) { list onTrackerAdded _ }
        case Messages.TrackerUpdated(node, previous) ⇒ list.ifConditionValid(node) { n ⇒ list onTrackerUpdated (n, previous) }
        case Messages.TrackerRemoved(node)           ⇒ list.ifConditionValid(node) { list onTrackerRemoved _ }
      }
    }).start()
  }

  private[borg] val trackers: ConcurrentMap[String, TrackerNode] = new ConcurrentHashMap[String, TrackerNode]()
  def get(id: String) = trackers.get(id)

  def apply(id: String) = get(id).get

  def set(node: TrackerNode) = registry.actorFor[TrackerRegistryActor] foreach { _ ! ('add, node) }

  private[borg] class TrackerRegistryMessageProvider extends ZooKeeperRegistryMessageProvider[Messages.TrackerRegistryMessage, String, TrackerNode] {
    def node(bytes: Array[Byte]) = TrackerNode(bytes)
    def addNode(subject: TrackerNode) = Messages.SetTracker(subject)
    def removeNode(subject: TrackerNode) = Messages.RemoveTracker(subject)
    def nodeAdded(subject: TrackerNode) = Messages.TrackerAdded(subject)
    def nodeUpdated(subject: TrackerNode, previous: TrackerNode) = Messages.TrackerUpdated(subject, previous)
    def nodeRemoved(subject: TrackerNode) = Messages.TrackerRemoved(subject)
  }

  private[borg] case class TrackerRegistryConfig(zookeeper: ZooKeeperClient) extends ZooKeeperRegistryConfig[Messages.TrackerRegistryMessage, String, TrackerNode] {
    val rootNode = "/trackers"
    val data = trackers
    val messageProvider = new TrackerRegistryMessageProvider
  }

  private[borg] class TrackerRegistryActor(config: TrackerRegistryConfig) extends ZooKeeperRegistry[Messages.TrackerRegistryMessage, String, TrackerNode](config)

}
