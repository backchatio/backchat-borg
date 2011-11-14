package backchat

import akka.actor.Uuid
import mojolly.LibraryImports


package object borg extends LibraryImports {

  val ApplicationEvent = mojolly.queue.ApplicationEvent
  type ApplicationEvent = mojolly.queue.ApplicationEvent

  def newCcId = new Uuid().toString

}