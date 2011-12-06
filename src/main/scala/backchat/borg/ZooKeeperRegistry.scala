package backchat.borg

import com.twitter.zookeeper.ZooKeeperClient
import akka.routing.Listeners
import akka.actor.Actor._
import org.apache.zookeeper.CreateMode
import collection.mutable.ConcurrentMap
import akka.actor.{ ActorRef, Actor }

trait Subject[IdType] extends MessageSerialization {
  def id: IdType
}
trait ZooKeeperRegistryMessageProvider[EventType, IdType, SubjectType <: Subject[IdType]] {

  def node(bytes: Array[Byte]): SubjectType
  def addNode(subject: SubjectType): EventType
  def removeNode(subject: SubjectType): EventType

  def nodeAdded(subject: SubjectType): EventType
  def nodeUpdated(subject: SubjectType, previousSubject: SubjectType): EventType
  def nodeRemoved(subject: SubjectType): EventType
}

trait ZooKeeperRegistryConfig[EventType, IdType, SubjectType <: Subject[IdType]] extends ZooKeeperRegistryMessageProvider[EventType, IdType, SubjectType] {
  def zookeeper: ZooKeeperClient
  def rootNode: String
  def data: ConcurrentMap[IdType, SubjectType]
  def messageProvider: ZooKeeperRegistryMessageProvider[EventType, IdType, SubjectType]
  def testProbe: Option[ActorRef] = None

  def node(bytes: Array[Byte]) = messageProvider.node(bytes)

  def addNode(subject: SubjectType) = messageProvider.addNode(subject)

  def removeNode(subject: SubjectType) = messageProvider.removeNode(subject)

  def nodeAdded(subject: SubjectType) = messageProvider.nodeAdded(subject)

  def nodeUpdated(subject: SubjectType, previous: SubjectType) = messageProvider.nodeUpdated(subject, previous)

  def nodeRemoved(subject: SubjectType) = messageProvider.nodeRemoved(subject)
}
abstract class ZooKeeperRegistry[EventType, IdType: Manifest, SubjectType <: Subject[IdType]: Manifest](context: ZooKeeperRegistryConfig[EventType, IdType, SubjectType]) extends Actor with Listeners with Logging {

  val data = context.data

  val zk = context.zookeeper

  override def preStart() {
    super.preStart()
    zk.watchChildren(context.rootNode, childrenChanged)
    context.testProbe foreach { _ ! 'initialized }
  }

  protected def receive = listenerManagement orElse manageNodes

  protected def manageNodes: Receive = {
    case ('add, node) if isSubject(node) ⇒ {
      val subj = node.asInstanceOf[SubjectType]
      if (zk.exists(nodePath(subj)))
        zk.set(nodePath(subj), subj.toBytes)
      else
        zk.create(nodePath(subj), subj.toBytes, CreateMode.EPHEMERAL)
    }
    case ('remove, node) if isSubject(node) ⇒ {
      val subj = node.asInstanceOf[SubjectType]
      zk delete nodePath(subj)
    }
  }

  protected def isSubject[TMessage: Manifest](node: TMessage) = manifest[TMessage] >:> manifest[SubjectType]

  def str2IdType(value: String): IdType = {
    val mf = manifest[IdType]
    if (mf <:< manifest[Long])
      value.toLong.asInstanceOf[IdType]
    else
      value.asInstanceOf[IdType]
  }

  protected def childrenChanged(children: Seq[String]) {
    val childrenSet = Set(children map str2IdType: _*)
    val watchedKeys = Set(data.keySet.toSeq: _*)
    val removedChildren = watchedKeys -- childrenSet
    val addedChildren = childrenSet -- watchedKeys

    val removedNodes = {
      val ch = data filterKeys removedChildren.contains
      data --= removedChildren
      ch.values.toSet
    }
    addedChildren foreach { child ⇒ zk.watchNode(childNode(child.toString), nodeChanged(child)) }
    removedNodes foreach { t ⇒ gossip(context.nodeRemoved(t)) }
  }

  protected def nodeChanged(child: IdType)(newData: Option[Array[Byte]]) {
    newData match {
      case Some(d) ⇒ {
        val nod = context.messageProvider.node(d)

        val previous = {
          val prev = data get child
          data(child) = nod
          prev
        }

        if (previous.isDefined) gossip(context.nodeUpdated(nod, previous.get))
        else gossip(context.nodeAdded(nod))
      }
      case None ⇒ // deletion handled via parent watch
    }
  }

  override protected def gossip(msg: Any) {
    context.testProbe foreach { _ ! msg }
    super.gossip(msg)
  }

  protected def nodePath(child: SubjectType) = childNode(child.id.toString)
  protected def childNode(child: String) = {
    (context.rootNode.endsWith("/"), child.startsWith("/")) match {
      case (true, true)   ⇒ context.rootNode + child.substring(1)
      case (false, false) ⇒ context.rootNode + "/" + child
      case _              ⇒ context.rootNode + child
    }
  }

}