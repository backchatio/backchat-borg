package backchat.borg

import mojolly.Enum
import net.liftweb.json._
import JsonDSL._

object Service {

  object DeploymentType extends Enum {
    sealed trait EnumVal extends Value {
      def pbType: Protos.Service.DeploymentType
    }

    val Jar = new EnumVal {
      val name = "Jar"
      val pbType = Protos.Service.DeploymentType.JAR
    }

    val RubyScript = new EnumVal {
      val name = "ruby-script"
      val pbType = Protos.Service.DeploymentType.RUBY_SCRIPT
    }

    val PythonScript = new EnumVal {
      val name = "python-script"
      val pbType = Protos.Service.DeploymentType.PYTHON_SCRIPT
    }

    val NodeJsScript = new EnumVal {
      val name = "node-js-script"
      val pbType = Protos.Service.DeploymentType.NODEJS_SCRIPT
    }

    val Gem = new EnumVal {
      val name = "gem"
      val pbType = Protos.Service.DeploymentType.GEM
    }

    val Egg = new EnumVal {
      val name = "egg"
      val pbType = Protos.Service.DeploymentType.EGG
    }

    val NPM = new EnumVal {
      val name = "npm"
      val pbType = Protos.Service.DeploymentType.NPM
    }

    val Deb = new EnumVal {
      val name = "deb"
      val pbType = Protos.Service.DeploymentType.DEB
    }

    def apply(protocol: Protos.Service.DeploymentType) = protocol match {
      case Protos.Service.DeploymentType.JAR           ⇒ Jar
      case Protos.Service.DeploymentType.RUBY_SCRIPT   ⇒ RubyScript
      case Protos.Service.DeploymentType.PYTHON_SCRIPT ⇒ PythonScript
      case Protos.Service.DeploymentType.NODEJS_SCRIPT ⇒ NodeJsScript
      case Protos.Service.DeploymentType.GEM           ⇒ Jar
      case Protos.Service.DeploymentType.EGG           ⇒ Egg
      case Protos.Service.DeploymentType.NPM           ⇒ NPM
      case Protos.Service.DeploymentType.DEB           ⇒ Deb
    }

  }

  def apply(bytes: Array[Byte]): Service = apply(Protos.Service.parseFrom(bytes))

  def apply(proto: Protos.Service): Service = {
    new Service(proto.getName, DeploymentType(proto.getDeployAs), ServiceType(proto.getProvides))
  }
}

import Service.DeploymentType
case class Service(
    name: String,
    deployedAs: DeploymentType.EnumVal = DeploymentType.Jar,
    provides: ServiceType.EnumVal = ServiceType.Domain) extends MessageSerialization {
  type ProtoBufMessage = Protos.Service

  override def toJValue: JValue = ("name" -> name) ~ ("deployedAs" -> deployedAs.name) ~ ("provides" -> provides.name)

  def toProtobuf = {
    Protos.Service.newBuilder().setName(name).setDeployAs(deployedAs.pbType).setProvides(provides.pbType).build()
  }
}