package backchat.borg

import mojolly.config.MojollyConfig


trait BorgConfig { self: MojollyConfig =>

  case class BorgConfig(hosts: String)

  val borg = BorgConfig(readString("mojolly.borg.hosts"))
}