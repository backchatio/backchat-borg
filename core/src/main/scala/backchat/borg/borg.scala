package backchat

import mojolly.LibraryImports

package object borg extends LibraryImports with org.scala_tools.time.Imports  {

  val ApplicationEvent = mojolly.queue.ApplicationEvent
  type ApplicationEvent = mojolly.queue.ApplicationEvent

}