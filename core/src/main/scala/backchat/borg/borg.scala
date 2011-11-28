package backchat

import mojolly.LibraryImports

package object borg extends LibraryImports with org.scala_tools.time.Imports  {

  val ApplicationEvent = mojolly.queue.ApplicationEvent
  type ApplicationEvent = mojolly.queue.ApplicationEvent

  def defaultValue[T: ClassManifest]: T = classManifest[T].erasure.toString match {
    case "void" => ().asInstanceOf[T]
    case "boolean" => false.asInstanceOf[T]
    case "byte" => (0: Byte).asInstanceOf[T]
    case "short" => (0: Short).asInstanceOf[T]
    case "char" => '\0'.asInstanceOf[T]
    case "int" => 0.asInstanceOf[T]
    case "long" => 0L.asInstanceOf[T]
    case "float" => 0.0F.asInstanceOf[T]
    case "double" => 0.0.asInstanceOf[T]
    case _ => null.asInstanceOf[T]
  }
}