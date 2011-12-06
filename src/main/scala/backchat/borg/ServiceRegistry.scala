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
import backchat.borg.ServiceRegistry.Messages.NodeMessage

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

  private[this] class AvailableNodesMessageProvider extends ZooKeeperRegistryMessageProvider[Messages.NodeMessage, Node] {
    def node(bytes: Array[Byte]) = Node(bytes)
    def addNode(subject: Node) = Messages.AddNode(subject)
    def removeNode(subject: Node) = Messages.RemoveNode(subject)
    def nodeAdded(subject: Node) = Messages.NodeAdded(subject)
    def nodeUpdated(subject: Node, previous: Node) = Messages.NodeUpdated(subject, previous)
    def nodeRemoved(subject: Node) = Messages.NodeRemoved(subject)
  }

  private[this] case class AvailableNodesConfig(zookeeper: ZooKeeperClient) extends ZooKeeperRegistryConfig[Messages.NodeMessage, Node] {
    val rootNode = "/members"

    val data = availableServers

    val messageProvider = new AvailableNodesMessageProvider
  }

  private class AvailableNodes(config: AvailableNodesConfig) extends ZooKeeperRegistry[Messages.NodeMessage, Node](config)

  private[this] class AvailableServicesMessageProvider extends ZooKeeperRegistryMessageProvider[Messages.ServiceMessage, Service] {
    def node(bytes: Array[Byte]) = Service(bytes)
    def addNode(subject: Service) = Messages.RegisterService(subject)
    def removeNode(subject: Service) = Messages.UnregisterService(subject)
    def nodeAdded(subject: Service) = Messages.ServiceAdded(subject)
    def nodeUpdated(subject: Service, previous: Service) = Messages.ServiceUpdated(subject, previous)
    def nodeRemoved(subject: Service) = Messages.ServiceRemoved(subject)
  }

  private[this] case class AvailableServicesConfig(zookeeper: ZooKeeperClient) extends ZooKeeperRegistryConfig[Messages.ServiceMessage, Service] {
    val rootNode = "/services"

    val data = availableServices

    val messageProvider = new AvailableServicesMessageProvider
  }

  private class AvailableServices(config: AvailableServicesConfig) extends ZooKeeperRegistry[Messages.ServiceMessage, Service](config)

  private[borg] class ServiceRegistryActor(context: ServiceRegistryContext) extends Actor with Listeners with Logging {

    import Messages._
    import Supervision._

    self.dispatcher = serviceRegistryDispatcher
    self.faultHandler = AllForOneStrategy(classOf[Throwable] :: Nil, 5, 10000)

    val zk = new ZooKeeperClient(context.zookeeperConfig)
    zk.connect()

    val members = actorOf(new AvailableNodes(AvailableNodesConfig(zk)))
    val services = actorOf(new AvailableServices(AvailableServicesConfig(zk)))

    protected def receive = listenerManagement orElse manageNodes

    protected def manageNodes: Receive = {
      case m: AddNode           ⇒ members ! ('add, m.node)
      case m: RemoveNode        ⇒ members ! ('remove, m.node)
      case m: RegisterService   ⇒ services ! ('add, m.service)
      case m: UnregisterService ⇒ services ! ('remove, m.service)
      case 'init ⇒ {
        context.testProbe foreach { _ ! 'initialized }
        self startLink members
        self startLink services
        logger info "Service registry has started"
      }
    }
  }
}