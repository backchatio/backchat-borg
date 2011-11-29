package backchat
package borg
package hive
package telepathy
package tests

import mojolly.testing.{AkkaSpecification, MojollySpecification}
import akka.actor.{Scheduler, Actor}
import akka.testkit.{TestProbe, TestKit}
import org.specs2.specification.{After, Fragments, Step}

trait ActorSpecification extends MojollySpecification {
  override def map(fs: => Fragments) =  super.map(fs) ^ Step(Actor.registry.shutdownAll()) //^ Step(Scheduler.restart())
}

class BinaryStarSpec extends ActorSpecification { def is =
  "A BinaryStar should" ^
    "when started as primary" ^
      "send the appropriate state as heartbeat" ! primary.sendsPeerPrimaryOnHeartbeat ^ bt ^
      "and a PeerBackup is received" ^
        "notify the listener to activate" ! pending ^
        "move to state active" ! pending ^ bt ^
      "and a PeerActive is received" ^
        "notify the listener to deactivate" ! pending ^
        "move to state passive" ! pending ^ bt ^
      "and a client request is received" ^
        "notify the listener to activate" ! pending ^
        "move to state active" ! pending ^
        "forward the message to the handler" ! pending ^ bt ^
    "when started as backup" ^
      "send the appropriate state as heartbeat" ! pending ^ end

  def primary = new PrimaryBinaryStartContext
  
  trait BinaryStarContext extends TestKit {
    import BinaryStar._
    
    def startAs: BinaryStar.BinaryStarRole
    val listener = TestProbe()
    lazy val starConfig = BinaryStarConfig(
      startAs,
      TelepathAddress("tcp://127.0.0.1:5010"),
      TelepathAddress("tcp://127.0.0.1:5011"),
      TelepathAddress("tcp://127.0.0.1:5012"),
      None)

    lazy val star = Actor.actorOf(new Reactor(starConfig)).start()

  }
  
  class PrimaryBinaryStartContext extends BinaryStarContext {

    val startAs = BinaryStar.Primary
    def sendsPeerPrimaryOnHeartbeat = {
      pending
    }
    
  }
}