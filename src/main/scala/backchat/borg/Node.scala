package backchat.borg

import collection.JavaConversions._

object Node {

  def apply(bytes: Array[Byte]): Node = {
    Node(Protos.Node parseFrom bytes)
  }

  def apply(proto: Protos.Node): Node = {
    new Node(
      proto.getId,
      proto.getUrl,
      Option(proto.getCapabilitiesList) map (Vector(_: _*)) getOrElse Vector.empty,
      Option(proto.getServicesList) map (Vector(_: _*)) getOrElse Vector.empty)
  }
}

case class Node(id: Long, url: String, capabilities: Seq[String], services: Seq[String]) extends Subject {

  type IdType = Long
  type ProtoBufMessage = Protos.Node

  def toProtobuf = {
    (Protos.Node.newBuilder
      setId id
      setUrl url
      addAllCapabilities capabilities
      addAllServices services).build()
  }
}