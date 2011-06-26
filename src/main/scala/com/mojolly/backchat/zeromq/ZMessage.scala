package com.mojolly.backchat
package zeromq

import java.nio.charset.Charset
import collection.mutable.ListBuffer
import annotation.tailrec

object ZMessage {

  val DELIMITER = Array[Byte]()
  var defaultCharset = Charset.forName("UTF-8")

  def apply(socket: Socket) = {
    @tailrec
    def read(result: ListBuffer[Array[Byte]] = ListBuffer[Array[Byte]]()): List[Array[Byte]] = {
      result += socket.recv(0)
      if (!socket.hasReceiveMore) { result.toList }
      else read(result)
    }
    new ZMessage(read())
  }

  def apply(body: String) = {
    new ZMessage(List(body.getBytes(defaultCharset)))
  }

  def apply(body: String, charset: Charset) = {
    new ZMessage(List(body.getBytes(charset)), charset)
  }

  def apply(bodyParts: String*) = {
    new ZMessage(bodyParts.map(_.getBytes(defaultCharset)).toList)
  }

  def apply(charset: Charset, bodyParts: String*) = {
    new ZMessage(bodyParts.map(_.getBytes(charset)).toList, charset)
  }

  //  def apply(body: Array[Byte]) = {
  //    new ZMessage(List(body))
  //  }
}
class ZMessage(parts: List[Array[Byte]] = Nil, val charset: Charset = ZMessage.defaultCharset) {

  import ZMessage.DELIMITER

  private var _parts = ListBuffer[Array[Byte]](parts: _*)

  def size = _parts.size

  def address = {
    new String(parts.headOption getOrElse DELIMITER, charset)
  }
  def address_=(data: String) { append(data.getBytes(charset)) }
  def body = new String(_parts.lastOption getOrElse DELIMITER, charset)
  def body_=(bd: String) {
    val bdb = bd.getBytes(charset)
    _parts.remove(_parts.size - 1)
    _parts.append(bdb)
  }

  private def get(index: Int) = {
    val ccb = if (_parts.size >= index) {
      _parts(size - index)
    } else DELIMITER
    new String(ccb, charset)
  }
  private def set(index: Int, value: String) {
    val mt = value.getBytes(charset)
    if (size > (index - 1)) {
      _parts.remove(size - index)
      _parts.insert(size - (index - 1), mt)
    } else push(mt)
  }

  def ccid = get(5)

  def ccid_=(mt: String) {
    set(5, mt)
  }

  def target = get(2)

  def target_=(mt: String) {
    set(2, mt)
  }

  def sender = get(3)

  def sender_=(mt: String) {
    set(3, mt)
  }

  def messageType = get(4)

  def messageType_=(mt: String) {
    set(4, mt)
  }

  def append(data: String): ZMessage = {
    append(data.getBytes(charset))
  }

  def push(data: String): ZMessage = {
    push(data.getBytes(charset))
  }
  def push(data: Array[Byte]): ZMessage = {
    _parts prepend data
    this
  }

  def pop() = {
    if (_parts.size > 0) new String(_parts.remove(0), charset)
    else ""
  }

  def addresses = {
    if (size > 5) _parts.dropRight(5).filterNot(_.isEmpty).toSeq
    else Seq.empty
  }

  def addresses_=(addr: Seq[Array[Byte]]) = {
    _parts = if (size > 5) { _parts.takeRight(5) } else _parts
    _parts prepend DELIMITER
    _parts.prepend(addr: _*)
    this
  }
  def append(data: Array[Byte]): ZMessage = {
    _parts.append(data)
    this
  }

  def unwrap() = {
    var address = _parts.remove(0)
    if (address.isEmpty) {
      address = _parts.remove(0)
    }
    if (_parts.headOption.forall(_.isEmpty)) {
      _parts.remove(0) // also remove the delimiter in one go
    }
    new String(address, charset)
  }

  def wrap(pts: String*) = {
    _parts.prepend(pts.map(_.getBytes(charset)): _*)
    this
  }

  def apply(socket: Socket) = {
    val mores = _parts.dropRight(1)
    val payload = _parts.takeRight(1).head
    mores foreach { socket.send(_, SendMore) }
    socket.send(payload, 0)
    this
  }

  override def toString = _parts.map(b ⇒ new String(b, charset)).mkString("ZMessage(", ", ", ")")
}