/**
 * Copyright (C) 2009-2011 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq

case class Frame(payload: Seq[Byte])
object Frame { def apply(s: String): Frame = Frame(s.getBytes) }

trait Deserializer[T] {
  def apply(frames: Seq[Frame]): T
}

class ZMQMessageDeserializer extends Deserializer[ZMQMessage] {
  def apply(frames: Seq[Frame]): ZMQMessage = ZMQMessage(frames)
}
