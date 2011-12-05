package backchat.borg

import akka.dispatch.Dispatchers
import akka.actor._
import Actor._
import com.twitter.zookeeper.{ ZooKeeperClientConfig, ZooKeeperClient }
import collection.JavaConversions._
import akka.routing.Listeners
import org.apache.zookeeper.CreateMode
import akka.config.Supervision
import akka.config.Supervision._
import java.util.concurrent.ConcurrentHashMap
import collection.mutable.ConcurrentMap

trait ServiceRegistryListener {

}

trait ServiceRegistryClient {

}

case class ServiceRegistryContext(
  zookeeperConfig: ZooKeeperClientConfig,
  poolSize: Int,
  membersNode: String = "/members",
  servicesNode: String = "/services",
  testProbe: Option[ActorRef] = None)

object ServiceRegistry {

  private val availableServers: ConcurrentMap[String, Node] = new ConcurrentHashMap[String, Node]()
  private val availableServices: ConcurrentMap[String, Service] = new ConcurrentHashMap[String, Service]

  private val serviceRegistryDispatcher =
    Dispatchers.newExecutorBasedEventDrivenWorkStealingDispatcher("service-registry-dispatch", 10).build

  private val serviceRegistrySupervisor = Supervisor(
    SupervisorConfig(OneForOneStrategy(List(classOf[Throwable]), 5, 10000), Nil))

  def start(context: ServiceRegistryContext) {
    serviceRegistrySupervisor.start
    context.poolSize times {
      val worker = actorOf(new ServiceRegistryActor(context))
      serviceRegistrySupervisor link worker
      worker.start()
    }
  }

  def stop {
    serviceRegistrySupervisor.shutdown()
  }

  object Messages {
    sealed trait ServiceRegistryMessage
    sealed trait NodeMessage extends ServiceRegistryMessage
    case class NodeAdded(node: Node) extends NodeMessage
    case class NodeUpdated(node: Node, previousValue: Node) extends NodeMessage
    case class NodeRemoved(node: Node) extends NodeMessage
    case class AddNode(node: Node) extends NodeMessage
    case class RemoveNode(node: Node) extends NodeMessage

    sealed trait ServiceMessage extends ServiceRegistryMessage
    case class ServiceAdded(service: Service) extends ServiceMessage
    case class ServiceUpdated(service: Service, previousValue: Service) extends ServiceMessage
    case class ServiceRemoved(service: Service) extends ServiceMessage
    case class RegisterService(service: Service) extends ServiceMessage
    case class UnregisterService(service: Service) extends ServiceMessage
  }

  private class AvailableNodes(zk: ZooKeeperClient, context: ServiceRegistryContext) extends Actor with Listeners with Logging {
    import Messages._

    zk.watchChildren(context.membersNode, childrenChanged)

    protected def receive = listenerManagement orElse manageNodes

    protected def manageNodes: Receive = {
      case AddNode(node) ⇒ {
        if (zk.exists(nodePath(node)))
          zk.set(nodePath(node), node.toBytes)
        else
          zk.create(nodePath(node), node.toBytes, CreateMode.EPHEMERAL)
      }
      case RemoveNode(node) ⇒ {
        zk delete nodePath(node)
      }
    }

    private def childrenChanged(children: Seq[String]) {
      val childrenSet = Set(children: _*)
      val watchedKeys = Set(availableServers.keySet.toSeq: _*)
      val removedChildren = watchedKeys -- childrenSet
      val addedChildren = childrenSet -- watchedKeys

      val removedNodes = {
        val ch = availableServers filterKeys removedChildren.contains
        availableServers --= removedChildren
        ch.values.toSet
      }
      addedChildren foreach { child ⇒ zk.watchNode(childNode(child), nodeChanged(child)) }
      removedNodes foreach { t ⇒ gossip(NodeRemoved(t)) }
    }

    private def nodeChanged(child: String)(newData: Option[Array[Byte]]) {
      newData match {
        case Some(d) ⇒ {
          val nod = Node(d)

          val previous = {
            val prev = availableServers get child
            availableServers(child) = nod
            prev
          }

          if (previous.isDefined) gossip(NodeUpdated(nod, previous.get))
          else gossip(NodeAdded(nod))
        }
        case None ⇒ // deletion handled via parent watch
      }
    }

    protected def nodePath(child: Node) = childNode(child.id.toString)
    protected def childNode(child: String) = {
      (context.membersNode.endsWith("/"), child.startsWith("/")) match {
        case (true, true)   ⇒ context.membersNode + child.substring(1)
        case (false, false) ⇒ context.membersNode + "/" + child
        case _              ⇒ context.membersNode + child
      }
    }

  }

  private class AvailableServices(zk: ZooKeeperClient, context: ServiceRegistryContext) extends Actor with Listeners with Logging {
    import Messages._

    zk.watchChildren(context.servicesNode, childrenChanged)

    protected def receive = listenerManagement orElse manageNodes

    protected def manageNodes: Receive = {
      case RegisterService(node) ⇒ {
        if (zk.exists(nodePath(node)))
          zk.set(nodePath(node), node.toBytes)
        else
          zk.create(nodePath(node), node.toBytes, CreateMode.EPHEMERAL)
      }
      case UnregisterService(node) ⇒ {
        zk delete nodePath(node)
      }
    }

    private def childrenChanged(children: Seq[String]) {
      val childrenSet = Set(children: _*)
      val watchedKeys = Set(availableServices.keySet.toSeq: _*)
      val removedChildren = watchedKeys -- childrenSet
      val addedChildren = childrenSet -- watchedKeys

      val removedNodes = {
        val ch = availableServices filterKeys removedChildren.contains
        availableServices -- removedChildren
        ch.values.toSet
      }
      addedChildren foreach { child ⇒ zk.watchNode(childNode(child), nodeChanged(child)) }
      removedNodes foreach { t ⇒ gossip(ServiceRemoved(t)) }
    }

    private def nodeChanged(child: String)(newData: Option[Array[Byte]]) {
      newData match {
        case Some(d) ⇒ {
          val nod = Service(d)

          val previous = {
            val prev = availableServices.get(child)
            availableServices(child) = nod
            prev
          }

          if (previous.isDefined) gossip(ServiceUpdated(nod, previous.get))
          else gossip(ServiceAdded(nod))
        }
        case None ⇒ // deletion handled via parent watch
      }
    }

    protected def nodePath(child: Service) = childNode(child.name)
    protected def childNode(child: String) = {
      (context.servicesNode.endsWith("/"), child.startsWith("/")) match {
        case (true, true)   ⇒ context.servicesNode + child.substring(1)
        case (false, false) ⇒ context.servicesNode + "/" + child
        case _              ⇒ context.servicesNode + child
      }
    }

  }

  private[borg] class ServiceRegistryActor(context: ServiceRegistryContext) extends Actor with Listeners with Logging {

    import Messages._
    import Supervision._

    self.dispatcher = serviceRegistryDispatcher
    self.faultHandler = AllForOneStrategy(classOf[Throwable] :: Nil, 5, 10000)

    val zk = new ZooKeeperClient(context.zookeeperConfig)
    zk.connect()

    val members = actorOf(new AvailableNodes(zk, context))
    val services = actorOf(new AvailableServices(zk, context))

    protected def receive = listenerManagement orElse manageNodes

    protected def manageNodes: Receive = {
      case m: AddNode           ⇒ members ! m
      case m: RemoveNode        ⇒ members ! m
      case m: RegisterService   ⇒ services ! m
      case m: UnregisterService ⇒ services ! m
      case 'init ⇒ {
        context.testProbe foreach { _ ! 'initialized }
        self startLink members
        self startLink services
        logger info "Service registry has started"
      }
    }
  }
}