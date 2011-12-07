package backchat.borg

import akka.actor._
import Actor._
import scalaz._
import Scalaz._
import com.twitter.zookeeper.ZooKeeperClientConfig
import collection.mutable
import telepathy.Messages.HiveRequest
import telepathy.{ TelepathAddress, TelepathClientConfig, Client }
import backchat.borg.{ Node ⇒ BorgNode }
import backchat.borg.ServiceRegistry.Messages.AddNode

case class HiveClientContext(clientId: String, hostName: String, ipAddress: String, port: Int, provides: Seq[String])
case class ActiveService(nodeId: String, connection: ActorRef)
object HiveClient {

  object Messages {
    sealed trait HiveClientMessage
    case class Connect(hostlist: String, paths: Seq[String]) extends HiveClientMessage
    case object Disconnect extends HiveClientMessage
  }

  private[borg] class HiveClientActor(context: HiveClientContext) extends Actor {

    import Messages._
    import context._

    self.id = clientId

    val node = BorgNode(hostName, "tcp://%s:%s".format(ipAddress, port), Nil, provides)

    protected var serviceRegistry: ActorRef = _
    private val activeServices = new mutable.HashMap[String, ActiveService]

    protected def isConnected = serviceRegistry.isNotNull && serviceRegistry.isRunning

    protected def receive = {
      case Connect(hostlist, _) ⇒ { // connect to zookeeper and set up watches for the require
        val context = ServiceRegistryContext(new ZooKeeperClientConfig {
          val hostList = hostlist
        })

        serviceRegistry = actorOf(new ServiceRegistry(context))
        self startLink serviceRegistry
        if (provides.nonEmpty) serviceRegistry ! AddNode(node)

        serviceRegistry ! ServiceRegistry.Messages.AddListener(new ServiceRegistryListener {
          override def nodeRemoved(node: BorgNode) = {
            val toRemove = activeServices filterKeys node.services.contains
            activeServices --= toRemove.keySet
          }
        })
      }
      case m: HiveRequest ⇒ {
        if (!(activeServices contains m.target))
          activateClientFor(m) foreach { activeServices(m.target) = _ }
        activeServices get m.target foreach { _.connection forward m }
      }
    }

    protected def activateClientFor(m: HiveRequest) = {
      nodeFor(m) map { addr ⇒
        ActiveService(addr.id, getOrCreateClient(addr.url))
      }
    }

    protected def getOrCreateClient(address: String) = {
      registry.actorsFor(address).headOption getOrElse {
        val config: TelepathClientConfig = TelepathClientConfig(TelepathAddress(address))
        val client = actorOf(createClient(config))
        self startLink client
        client
      }
    }

    protected def createClient(config: TelepathClientConfig): Actor = new Client(config)

    protected def nodeFor(m: HiveRequest) = {
      (serviceRegistry ? ServiceRegistry.Messages.NodeForService(m.target)).as[Any] match {
        case Some(ServiceRegistry.Messages.ServiceUnavailable) | None ⇒ None
        case Some(n: BorgNode)                                        ⇒ Some(n)
      }
    }
  }
}