package backchat.borg

import mojolly.Enum

object ServiceType extends Enum {
  sealed trait EnumVal extends Value {
    def pbType: Protos.ServiceType
  }

  val Domain = new EnumVal {
    val name = "domain"
    val pbType = Protos.ServiceType.DOMAIN
  }

  val Streams = new EnumVal {
    val name = "streams"
    val pbType = Protos.ServiceType.STREAMS
  }

  val Indexer = new EnumVal {
    val name = "indexer"
    val pbType = Protos.ServiceType.INDEXER
  }

  val Tracker = new EnumVal {
    val name = "tracker"
    val pbType = Protos.ServiceType.TRACKER
  }

  val Publisher = new EnumVal {
    val name = "publisher"
    val pbType = Protos.ServiceType.TRACKER
  }

  def apply(protocol: Protos.ServiceType) = protocol match {
    case Protos.ServiceType.DOMAIN    ⇒ Domain
    case Protos.ServiceType.STREAMS   ⇒ Streams
    case Protos.ServiceType.INDEXER   ⇒ Indexer
    case Protos.ServiceType.TRACKER   ⇒ Tracker
    case Protos.ServiceType.PUBLISHER ⇒ Publisher
  }

}