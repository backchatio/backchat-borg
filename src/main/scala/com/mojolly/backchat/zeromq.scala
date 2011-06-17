package com.mojolly.backchat

import org.zeromq.{ ZMQ ⇒ JZMQ }
import akka.actor.Uuid
import net.liftweb.json.JsonAST.JValue

package object zeromq extends org.scala_tools.time.Implicits {

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

  def newCcId = new Uuid().toString

  implicit def jvalueToBCJValue(jv: JValue) = new CoreExtensions.BackchatJValue(jv)
}