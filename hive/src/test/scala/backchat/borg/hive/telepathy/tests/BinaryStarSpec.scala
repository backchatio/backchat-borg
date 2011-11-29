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
import telepathy.BinaryStar.Messages._
import telepathy.Messages.Ping

trait ActorSpecification extends MojollySpecification {
  override def map(fs: => Fragments) =  super.map(fs) ^ Step(Actor.registry.shutdownAll()) ^ Step(Scheduler.restart())
}

class BinaryStarSpec extends ActorSpecification { def is =
  "A BinaryStar should" ^
    "when started as primary" ^
      "send the appropriate state as heartbeat" ! primary.sendsPeerPrimaryOnHeartbeat ^
      "and a PeerBackup is received" ^
        "notify the listener to activate" ! primary.notifiesListenerToActivate(PeerBackup) ^
        "move to state active" ! primary.goesToActivate(PeerBackup) ^ bt ^
      "and a PeerActive is received" ^
        "notify the listener to deactivate" ! primary.notifiesListenerToDeactivate ^
        "move to state passive" ! primary.goesToPassive(PeerActive) ^ bt ^
      "and a client request is received" ^
        "notify the listener to activate" ! primary.notifiesListenerToActivate(ClientRequest(Ping)) ^
        "move to state active" ! primary.goesToActivate(ClientRequest(Ping)) ^
        "forward the message to the handler" ! primary.forwardsRequest ^ bt ^
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
    lazy val defaultConfig = BinaryStarConfig(startAs,
                                              defaultVoter,
                                              defaultPub,
                                              defaultSub,
                                              None)
    def withStar[T](voter: TelepathAddress = defaultVoter, stateSub: TelepathAddress = defaultSub, statePub: TelepathAddress = defaultPub)(fn: ActorRef => T)(implicit evidence$1: (T) => Result): T = {
      val cfg = BinaryStarConfig(
            startAs,
            voter,
            statePub,
            stateSub,
            None)
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
        val latch = TestLatch()
        server onMessage { frames =>
          Messages(zmqMessage(frames)) match {
            case PeerPrimary => latch.countDown()
            case _ =>
          }
        }
        withStar(statePub = server.address) { star =>
          star ! BinaryStar.Messages.Heartbeat
          server.poll(2.seconds)
          latch.await(10.millis) must beTrue
        }
      }
    }
    
    def notifiesListenerToActivate(msg: Any) = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
      fsm ! msg
      receiveOne(2.seconds) must_== Active
    }

    def goesToActivate(msg: Any) = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig)).start
      fsm ! msg
      fsm.stateName must be_==(Active).eventually
    }

    def goesToPassive(msg: Any) = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig)).start
      fsm ! msg
      fsm.stateName must be_==(Passive).eventually
    }

    def notifiesListenerToDeactivate = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
      fsm ! PeerActive
      receiveOne(2.seconds) must_== Passive
    }
    
    def forwardsRequest = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
      fsm ! ClientRequest(Ping)
      receiveN(2, 2.seconds).last must_== Ping
    }
    
  }
}