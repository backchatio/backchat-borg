package com.mojolly.backchat

import org.zeromq.{ ZMQ ⇒ JZMQ }
import akka.actor.Uuid
import net.liftweb.json.JsonAST.JValue
import java.util.Date
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTimeZone, DateTime}
import net.liftweb.json.{Formats, DateFormat, DefaultFormats}
import net.liftweb.json.ext.JodaTimeSerializers

package zeromq {
  class BackchatFormats extends DefaultFormats {
    override val dateFormat = new DateFormat {

      def format(d: Date) = new DateTime(d).toString(ISO8601_DATE)

      def parse(s: String) = try {
        Option(ISO8601_DATE.parseDateTime(s).toDate)
      } catch {
        case _ ⇒ None
      }
    }

  }
}

package object zeromq {

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
  private[zeromq] val MIN_DATE = new DateTime(0L)
  private[zeromq] val ISO8601_DATE = ISODateTimeFormat.dateTime.withZone(DateTimeZone.UTC)
  private[zeromq] implicit val formats: Formats = new com.mojolly.backchat.zeromq.BackchatFormats ++ JodaTimeSerializers.all

  def newCcId = new Uuid().toString

  private[zeromq] implicit def jvalueToBCJValue(jv: JValue) = new CoreExtensions.BackchatJValue(jv)
  private[zeromq] implicit def string2BCString(s: String) = new CoreExtensions.BackchatString(s)
}