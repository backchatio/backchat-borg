/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

import org.zeromq.{ ZMQ ⇒ JZMQ }

object SocketType extends Enumeration {
  type SocketType = Value
  val Pub = Value(JZMQ.PUB)
  val Sub = Value(JZMQ.SUB)
  val Dealer = Value(JZMQ.XREQ)
  val Router = Value(JZMQ.XREP)
  val Req = Value(JZMQ.REQ)
  val Rep = Value(JZMQ.REP)
  val Push = Value(JZMQ.PUSH)
  val Pull = Value(JZMQ.PULL)
  val Pair = Value(JZMQ.PAIR)
}
