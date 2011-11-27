package backchat
package borg
package hive

import akka.actor._
import Actor._
import akka.dispatch.Dispatchers
import rl.Uri
import akka.config.Supervision._

package object telepathy {

  def newCcid = new Uuid
  val telepathyDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("borg-telepathy-dispatcher").build

  private val supervisor =
    Supervisor(SupervisorConfig(OneForOneStrategy(classOf[Throwable] :: Nil, 5, 3000), Nil))

  def newTelepathicClient(server: String): ActorRef = {
    val uri = Uri(server)
    val auth = uri.authority.get
    val addr = TelepathAddress(auth.host.value, auth.port.get)
    val client = actorOf(new Client(TelepathClientConfig(addr)))
    supervisor link client
    client.start
  }
}