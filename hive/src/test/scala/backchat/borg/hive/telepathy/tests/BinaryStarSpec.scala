package backchat
package borg
package hive
package telepathy
package tests

import mojolly.testing.{AkkaSpecification, MojollySpecification}
import org.specs2.specification.{After, Fragments, Step}
import org.zeromq.ZMQ
import akka.actor.{ActorRef, Scheduler, Actor}
import org.specs2.execute.Result
import mojolly.io.FreePort
import akka.testkit._

trait ActorSpecification extends MojollySpecification {
  override def map(fs: => Fragments) =  super.map(fs) ^ Step(Actor.registry.shutdownAll()) // ^ Step(Scheduler.restart())
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

    lazy val port = FreePort.randomFreePort(50)
    lazy val defaultVoter = TelepathAddress("127.0.0.1", port)
    lazy val defaultSub = TelepathAddress("127.0.0.1", port + 1)
    lazy val defaultPub = TelepathAddress("127.0.0.1", port + 2)
    def withStar[T](voter: TelepathAddress = defaultVoter, stateSub: TelepathAddress = defaultSub, statePub: TelepathAddress = defaultPub)(fn: ActorRef => T)(implicit evidence$1: (T) => Result): T = {
      val cfg = BinaryStarConfig(
            startAs,
            voter,
            statePub,
            stateSub,
            None)
      println("creating binary star with: %s" format cfg)
      val st = Actor.actorOf(new Reactor(cfg)).start()
      fn(st)
    }

  }
  
  class PrimaryBinaryStartContext extends BinaryStarContext with ZeroMqContext {

    import BinaryStar._
    import Messages._
    val startAs = BinaryStar.Primary

    def sendsPeerPrimaryOnHeartbeat = this {
      withServer(ZMQ.SUB) { server =>
        sleep -> 500.millis
        val latch = TestLatch()
        server onMessage { frames =>
          println("received frames on the server")
          Messages(zmqMessage(frames)) match {
            case PeerPrimary => latch.countDown()
            case m => sys.error("Received unknown message: %s".format(m))
          }
        }
        withStar(statePub = server.address) { star =>
          star ! BinaryStar.Messages.Heartbeat
          server.poll(2.seconds)
          latch.await(TestLatch.DefaultTimeout) must beTrue
        }
      }
    }
    
  }
}