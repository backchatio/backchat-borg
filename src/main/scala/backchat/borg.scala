package backchat

import org.zeromq.{ ZMQ ⇒ JZMQ }
import akka.actor.Uuid

package object borg extends mojolly.LibraryImports {

  type ZMQ = JZMQ
  type Socket = JZMQ.Socket
  type Context = JZMQ.Context
  val Router = JZMQ.XREP
  val Dealer = JZMQ.XREQ
  val Req = JZMQ.REQ
  val Rep = JZMQ.REP
  val Push = JZMQ.PUSH
  val Pull = JZMQ.PULL
  val Pub = JZMQ.PUB
  val Sub = JZMQ.SUB
  val Pair = JZMQ.PAIR
  val SendMore = JZMQ.SNDMORE
  val NoBlock = JZMQ.NOBLOCK
  val ApplicationEvent = mojolly.queue.ApplicationEvent
  type ApplicationEvent = mojolly.queue.ApplicationEvent

  def newCcId = new Uuid().toString

}