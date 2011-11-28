package backchat
package borg
package hive
package telepathy

import akka.zeromq._

trait FromZMQMessage[TMessage] {
  def fromZMQMessage(msg: ZMQMessage): TMessage
}

trait ToZMQMessage[TMessage] {
  def toZMQMessage(msg: TMessage): ZMQMessage
}

trait ZMQMessageFormat[TMessage] extends FromZMQMessage[BorgMessage] with ToZMQMessage[BorgMessage]

class BorgZMQMessageSerializer extends ZMQMessageFormat[BorgMessage] with Deserializer with Logging {
  def fromZMQMessage(msg: ZMQMessage) = {
    logger debug "Received [%d] frames".format(msg.frames.size)
    BorgMessage(msg.frames.last.payload)
  }
  def toZMQMessage(msg: BorgMessage) = ZMQMessage(msg.toProtobuf)

  def apply(frames: Seq[Frame]): Any = fromZMQMessage(ZMQMessage(frames))
}
