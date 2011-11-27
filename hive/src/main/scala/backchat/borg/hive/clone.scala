package backchat.borg.hive

import akka.dispatch.Dispatchers
import akka.actor.Uuid


package object clone {

  val cloneDispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("borg-clone-dispatcher").build
}