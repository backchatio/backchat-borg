package backchat
package borg
package cadence

import akka.actor.{Actor, Scheduler}


object AlerterApp extends App {
  import Alerter._
  val poller = new Alerter(new AlerterConfig)
  poller onLoad()
  sys.addShutdownHook {
    poller onUnload()
    Scheduler shutdown()
    Actor.registry shutdownAll()
  }
}