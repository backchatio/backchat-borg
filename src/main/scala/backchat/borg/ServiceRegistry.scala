package backchat.borg

import akka.actor._
import Actor._
import com.twitter.zookeeper.{ ZooKeeperClientConfig, ZooKeeperClient }
import collection.JavaConversions._
import akka.config.Supervision
import Supervision._
import collection.mutable.{ ConcurrentMap, HashSet }
import akka.routing.{ Listen, Listeners }

trait ServiceRegistryListener {

  def nodeAdded(node: Node) {}
  def nodeUpdated(node: Node, previous: Node) {}
  def nodeRemoved(node: Node) {}

  def serviceAdded(service: Service) {}
  def serviceUpdated(service: Service, previous: Service) {}
  def serviceRemoved(service: Service) {}
}

trait ServiceRegistryClient {

  import ServiceRegistry.Messages._

  protected def context: ServiceRegistryContext
  protected def proxy = registry.actorFor[ServiceRegistry] getOrElse actorOf(new ServiceRegistry(context)).start()
  def node: Node

  def join() { proxy ! AddNode(node) }
  def leave() { proxy ! RemoveNode(node) }

  def registerService(service: Service) { proxy ! RegisterService(service) }
  def unregisterService(service: Service) { proxy ! UnregisterService(service) }

  def getNode(id: String) = (proxy ? GetNode(id)).as[Node]
  def getService(name: String) = (proxy ? GetService(name)).as[Service]

  def nodeExists(id: String) = getNode(id).isDefined
  def serviceExists(name: String) = getService(name).isDefined

  def addListener(listener: ServiceRegistryListener) { proxy ! AddListener(listener) }
  def removeListener(listener: ServiceRegistryListener) { proxy ! RemoveListener(listener) }

}

case class ServiceRegistryContext(
  zookeeperConfig: ZooKeeperClientConfig,
  members: ConcurrentMap[String, Node] = mapMaker.makeMap[String, Node],
  services: ConcurrentMap[String, Service] = mapMaker.makeMap[String, Service](),
  loadBalancerFor: Iterable[Node] => LoadBalancer = LoadBalancer.FirstRegistered,
  serviceFor: String => Option[Node] = ServiceUnavailable,
  testProbe: Option[ActorRef] = None)

object ServiceRegistry {

  object Messages {
    sealed trait ServiceRegistryMessage
    sealed trait ServiceRegistryEvent
    sealed trait NodeMessage extends ServiceRegistryMessage
    sealed trait NodeEvent extends ServiceRegistryEvent with NodeMessage
    case class NodeAdded(node: Node) extends NodeEvent
    case class NodeUpdated(node: Node, previousValue: Node) extends NodeEvent
    case class NodeRemoved(node: Node) extends NodeEvent
    case class AddNode(node: Node) extends NodeMessage
    case class RemoveNode(node: Node) extends NodeMessage
    case class GetNode(id: String) extends NodeMessage
    case class AddressFor(service: String) extends NodeMessage
    case class NodeForService(service: String) extends NodeMessage

    case class AddListener(listener: ServiceRegistryListener) extends ServiceRegistryMessage
    case class RemoveListener(listener: ServiceRegistryListener) extends ServiceRegistryMessage

    sealed trait ServiceMessage extends ServiceRegistryMessage
    sealed trait ServiceEvent extends ServiceRegistryEvent with ServiceMessage
    case class ServiceAdded(service: Service) extends ServiceEvent
    case class ServiceUpdated(service: Service, previousValue: Service) extends ServiceEvent
    case class ServiceRemoved(service: Service) extends ServiceEvent
    case class RegisterService(service: Service) extends ServiceMessage
    case class GetService(name: String) extends ServiceMessage
    case class UnregisterService(service: Service) extends ServiceMessage
    case object ServiceUnavailable extends ServiceMessage
  }

  private[borg] class AvailableNodesMessageProvider extends ZooKeeperRegistryMessageProvider[Messages.NodeMessage, String, Node] {
    def node(bytes: Array[Byte]) = Node(bytes)
    def addNode(subject: Node) = Messages.AddNode(subject)
    def removeNode(subject: Node) = Messages.RemoveNode(subject)
    def nodeAdded(subject: Node) = Messages.NodeAdded(subject)
    def nodeUpdated(subject: Node, previous: Node) = Messages.NodeUpdated(subject, previous)
    def nodeRemoved(subject: Node) = Messages.NodeRemoved(subject)
  }

  private[borg] case class AvailableNodesConfig(zookeeper: ZooKeeperClient, data: ConcurrentMap[String, Node]) extends ZooKeeperRegistryConfig[Messages.NodeMessage, String, Node] {
    val rootNode = "/members"

    val messageProvider = new AvailableNodesMessageProvider
  }

  private class AvailableNodes(config: AvailableNodesConfig) extends ZooKeeperRegistry[Messages.NodeMessage, String, Node](config)

  private[borg] class AvailableServicesMessageProvider extends ZooKeeperRegistryMessageProvider[Messages.ServiceMessage, String, Service] {
    def node(bytes: Array[Byte]) = Service(bytes)
    def addNode(subject: Service) = Messages.RegisterService(subject)
    def removeNode(subject: Service) = Messages.UnregisterService(subject)
    def nodeAdded(subject: Service) = Messages.ServiceAdded(subject)
    def nodeUpdated(subject: Service, previous: Service) = Messages.ServiceUpdated(subject, previous)
    def nodeRemoved(subject: Service) = Messages.ServiceRemoved(subject)
  }

  private[borg] case class AvailableServicesConfig(zookeeper: ZooKeeperClient, data: ConcurrentMap[String, Service]) extends ZooKeeperRegistryConfig[Messages.ServiceMessage, String, Service] {
    val rootNode = "/services"

    val messageProvider = new AvailableServicesMessageProvider
  }

  private class AvailableServices(config: AvailableServicesConfig) extends ZooKeeperRegistry[Messages.ServiceMessage, String, Service](config)

  private[borg] class ServiceRegistryActor(context: ServiceRegistryContext) extends Actor with Listeners with Logging {

    import Messages._
    import Supervision._

    self.faultHandler = AllForOneStrategy(classOf[Throwable] :: Nil, 5, 10000)

    val zk = new ZooKeeperClient(context.zookeeperConfig)
    zk.connect()

    val members = actorOf(new AvailableNodes(AvailableNodesConfig(zk, context.members)))
    val services = actorOf(new AvailableServices(AvailableServicesConfig(zk, context.services)))

    protected def receive = listenerManagement orElse manageNodes orElse forwardCallbacks

    protected def forwardCallbacks: Receive = {
      case m: NodeEvent    ⇒ gossip(m)
      case m: ServiceEvent ⇒ gossip(m)
    }

    protected def manageNodes: Receive = {
      case m: AddNode           ⇒ members ! ('add, m.node)
      case m: RemoveNode        ⇒ members ! ('remove, m.node)
      case m: RegisterService   ⇒ services ! ('add, m.service)
      case m: UnregisterService ⇒ services ! ('remove, m.service)
      case 'init ⇒ {
        context.testProbe foreach { _ ! 'initialized }
        self startLink members
        self startLink services
        members ! Listen(self)
        services ! Listen(self)
        logger info "Service registry has started"
      }
    }
  }
}

class ServiceRegistry(context: ServiceRegistryContext) extends Actor with Logging {

  import ServiceRegistry._
  import Messages._

  self.faultHandler = OneForOneStrategy(List(classOf[Throwable]), 5, 10000)

  private val availableServers: ConcurrentMap[String, Node] = context.members
  private val availableServices: ConcurrentMap[String, Service] = context.services
  private val listeners = HashSet[ServiceRegistryListener]()

  private lazy val worker = actorOf(new ServiceRegistryActor(context))

  override def preStart() {
    self startLink worker
    worker ! Listen(self)
  }

  protected def receive = manageRegistry

  protected def notifyListeners(m: ServiceRegistryEvent) = listeners foreach { l ⇒
    m match {
      case NodeAdded(node)                   ⇒ l nodeAdded node
      case NodeUpdated(node, previous)       ⇒ l.nodeUpdated(node, previous)
      case NodeRemoved(node)                 ⇒ l nodeRemoved node
      case ServiceAdded(service)             ⇒ l serviceAdded service
      case ServiceUpdated(service, previous) ⇒ l serviceUpdated (service, previous)
      case ServiceRemoved(service)           ⇒ l serviceRemoved service
    }
  }

  protected def manageRegistry: Receive = {
    case AddListener(listener)    ⇒ listeners += listener
    case RemoveListener(listener) ⇒ listeners -= listener
    case GetNode(id)              ⇒ self tryReply (availableServers get id)
    case GetService(name)         ⇒ self tryReply (availableServices get name)
    case NodeForService(service) ⇒ {
      val foundNode = context loadBalancerFor availableServers.values nodeFor service
      self tryReply (foundNode getOrElse context.serviceFor(service) )
    }
    case m: ServiceRegistryEvent ⇒ notifyListeners(m)
    case m: NodeMessage          ⇒ worker forward m
    case m: ServiceMessage       ⇒ worker forward m
  }
}