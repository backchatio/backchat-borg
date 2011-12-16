package backchat.borg

import akka.actor._
import Actor._
import akka.dispatch.Dispatchers
import akka.config.Supervision._

package object telepathy {

  val telepathyDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("borg-telepathy-dispatcher").build

  def newSocketDispatcher(actor: ActorRef) = Dispatchers.newThreadBasedDispatcher(actor)

  private val supervisor =
    Supervisor(SupervisorConfig(OneForOneStrategy(classOf[Throwable] :: Nil, 5, 3000), Nil))

  def newTelepathicClient(server: String, supervisor: { def link(actor: ActorRef) } = supervisor): ActorRef = {
    val addr = TelepathAddress(server)
    val client = actorOf(new Client(TelepathClientConfig(addr)))
    supervisor link client
    client.start
  }
}