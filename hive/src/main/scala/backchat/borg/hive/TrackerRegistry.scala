package backchat
package borg
package hive

import akka.actor._
import Actor._
import scalaz._
import Scalaz._
import collection.JavaConversions._
import com.twitter.zookeeper.{ ZooKeeperClient, ZooKeeperClientConfig }
import org.apache.zookeeper.CreateMode
import akka.stm._
import akka.routing.{ Deafen, Listen, Listeners }

trait TrackerRegistryListener {

  def condition(node: TrackerRegistry.TrackerNode) = true
  def ifConditionValid(node: TrackerRegistry.TrackerNode)(thunk: TrackerRegistry.TrackerNode ⇒ Any) {
    if (condition(node)) thunk(node)
  }

  def onTrackerAdded(node: TrackerRegistry.TrackerNode) {

  }
  def onTrackerUpdated(node: TrackerRegistry.TrackerNode) {

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
      TrackerNode(proto.getId, proto.getSource, Option(proto.getKindList) map (_.toList) getOrElse Nil, proto.getNodeId)
    }
  }

  case class TrackerNode(id: String, source: String, kinds: Seq[String], nodeId: Long) extends MessageSerialization {
    type ProtoBufMessage = Protos.TrackerNode

    def toProtobuf = {
      Protos.TrackerNode.newBuilder.setId(id).setSource(source).addAllKind(kinds).setNodeId(nodeId).build()
    }
  }
  object Messages {

    sealed trait TrackerRegistryMessage
    case class GetTracker(id: String) extends TrackerRegistryMessage
    case class SetTracker(node: TrackerNode) extends TrackerRegistryMessage
    case class TrackerAdded(node: TrackerNode) extends TrackerRegistryMessage
    case class TrackerUpdated(node: TrackerNode) extends TrackerRegistryMessage
    case class TrackerRemoved(node: TrackerNode) extends TrackerRegistryMessage
  }

  def start(config: ZooKeeperClientConfig, supervisor: Option[ActorRef] = None) {
    val reg = supervisor map { sup ⇒
      val a = actorOf(new TrackerRegistryActor(config))
      sup startLink a
      a
    } getOrElse {
      actorOf(new TrackerRegistryActor(config)).start()
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
        case Messages.TrackerAdded(node)   ⇒ list.ifConditionValid(node) { list onTrackerAdded _ }
        case Messages.TrackerUpdated(node) ⇒ list.ifConditionValid(node) { list onTrackerUpdated _ }
        case Messages.TrackerRemoved(node) ⇒ list.ifConditionValid(node) { list onTrackerRemoved _ }
      }
    }).start()
  }

  def get(id: String) = registry.actorFor[TrackerRegistryActor] flatMap {
    r ⇒ (r ? Messages.GetTracker(id)).as[Option[TrackerNode]] getOrElse None
  }

  def apply(id: String) = get(id).get

  def set(node: TrackerNode) = registry.actorFor[TrackerRegistryActor] foreach { _ ! Messages.SetTracker(node) }

  private[hive] class TrackerRegistryActor(config: ZooKeeperClientConfig, testProbe: Option[ActorRef] = None) extends Actor with Logging with Listeners {

    import Messages._

    var data = TransactionalMap[String, TrackerNode]()
    val zk = new ZooKeeperClient(config)
    zk.connect()

    override protected def gossip(msg: Any) {
      testProbe foreach { _ ! msg }
      super.gossip(msg)
    }

    override def preStart() {
      self ! 'init
    }

    protected def receive = listenerManagement orElse nodeManagement

    protected def nodeManagement: Receive = {
      case GetTracker(trackerId) ⇒ {
        val fetched = atomic { data.get(trackerId) }
        self tryReply fetched
      }
      case SetTracker(nod) ⇒ {
        if (zk.exists(node(nod)))
          zk.set(node(nod), nod.toBytes)
        else
          zk.create(node(nod), nod.toBytes, CreateMode.EPHEMERAL)
      }
      case 'init ⇒ {
        zk.watchChildren(config.rootNode, childrenChanged)
        testProbe foreach { _ ! 'initialized }
        logger info "Tracker registry has started"
      }
    }

    private def childrenChanged(children: Seq[String]) {
      val childrenSet = Set(children: _*)
      val watchedKeys = atomic { Set(data.keySet.toSeq: _*) }
      val removedChildren = watchedKeys -- childrenSet
      val addedChildren = childrenSet -- watchedKeys

      val removedNodes = atomic {
        val ch = data filterKeys removedChildren.contains
        data -- removedChildren
        ch.values.toSet
      }

      addedChildren foreach { child ⇒
        zk.watchNode(node(child), nodeChanged(child))
      }
      removedNodes foreach { t ⇒ gossip(TrackerRemoved(t)) }
    }

    private def nodeChanged(child: String)(newData: Option[Array[Byte]]) {
      newData match {
        case Some(d) ⇒ {
          val nod = TrackerNode(d)
          var isUpdate = true

          atomic {
            isUpdate = data.containsKey(child)
            data(child) = nod
          }

          if (isUpdate) gossip(TrackerUpdated(nod))
          else gossip(TrackerAdded(nod))
        }
        case None ⇒ // deletion handled via parent watch
      }
    }

    private def node(nod: String): String = {
      (config.rootNode.endsWith("/"), nod.startsWith("/")) match {
        case (true, true)   ⇒ config.rootNode + nod.substring(1)
        case (false, false) ⇒ config.rootNode + "/" + nod
        case _              ⇒ config.rootNode + nod
      }
    }

    private def node(nod: TrackerNode): String = node(nod.id)

  }

}
