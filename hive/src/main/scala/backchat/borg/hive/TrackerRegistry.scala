package backchat
package borg
package hive

import akka.actor._
import Actor._
import scalaz._
import Scalaz._
import collection.mutable
import collection.JavaConversions._
import com.twitter.zookeeper.{ ZooKeeperClient, ZooKeeperClientConfig }
import akka.routing.Listeners
import org.apache.zookeeper.CreateMode

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
  }

  def get(id: String) = registry.actorFor[TrackerRegistryActor] flatMap {
    r ⇒ (r ? Messages.GetTracker(id)).as[Option[TrackerNode]] getOrElse None
  }

  def apply(id: String) = get(id).get

  def set(node: TrackerNode) = registry.actorFor[TrackerRegistryActor] foreach { _ ! Messages.SetTracker(node) }

  class TrackerRegistryActor(config: ZooKeeperClientConfig, testProbe: Option[ActorRef] = None) extends Actor with Logging with Listeners {

    import Messages._

    val data = new mutable.HashMap[String, TrackerNode]()
    val zk = new ZooKeeperClient(config)
    logger debug "initializing"
    zk.connect()
    logger debug "connected"

    override protected def gossip(msg: Any) {
      testProbe foreach { _ ! msg }
      super.gossip(msg)
    }

    private def readBytes(bytes: Array[Byte]) = {
      val n = TrackerNode(bytes)
      gossip(n)
      n
    }

    override def preStart() {
      self ! 'init
    }

    protected def receive = listenerManagement orElse nodeManagement

    protected def nodeManagement: Receive = {
      case GetTracker(trackerId) ⇒ {
        logger debug "Fetching: %s, The keys: %s".format(trackerId, data.keys.mkString(", "))
        val fetched = data.get(trackerId)
        logger debug "Fetched: %s".format(fetched)
        self tryReply fetched
      }
      case SetTracker(nod) ⇒ {
        if (zk.exists(node(nod)))
          zk.set(node(nod), nod.toBytes)
        else
          zk.create(node(nod), nod.toBytes, CreateMode.EPHEMERAL)
      }
      case 'init ⇒ {
        zk.watchChildrenWithData[TrackerNode](config.rootNode, data, readBytes _, (s: String) ⇒ logger.debug(s))
        logger debug "set the watcher"
        testProbe foreach { _ ! 'initialized }
        logger debug "initialized"
      }
    }

    private def node(nod: TrackerNode) =
      if (config.rootNode.endsWith("/")) config.rootNode + nod.id
      else config.rootNode + "/" + nod.id

  }

}
