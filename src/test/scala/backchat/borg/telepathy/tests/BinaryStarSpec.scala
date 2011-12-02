package backchat
package borg
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

class BinaryStarSpec extends AkkaSpecification { def is =
  "A BinaryStar should" ^
    "when started as primary" ^ t ^
      sendsHearbeat(primary, PeerPrimary) ^
      "and a PeerBackup is received" ^
        "notify the listener to activate" ! primary.notifiesListenerToActivate(PeerBackup) ^
        "move to state active" ! primary.goesToActivate(PeerBackup) ^ bt ^
      `and a PeerActive is received`(primary) ^
      "and a client request is received" ^
        "notify the listener to activate" ! primary.notifiesListenerToActivate(ClientRequest(Ping)) ^
        "move to state active" ! primary.goesToActivate(ClientRequest(Ping)) ^
        "forward the message to the handler" ! primary.forwardsRequest ^ bt ^ bt ^
    "when started as backup" ^ t ^
      sendsHearbeat(backup, PeerBackup) ^
      `and a PeerActive is received`(backup) ^
      "shuts down on error" ! backup.shutsDownOn(ClientRequest(Ping)) ^ bt ^
    "when running in Passive mode" ^ t ^
      sendsHearbeat(passive, PeerPassive) ^
      "notify the listener to activate when transitioning to Active" ! passive.notifiesOnTransition ^
      "shuts down on error" ! passive.shutsDownOn(PeerPassive) ^ bt ^
    "when running in Active mode" ^ t ^
      "shuts down on error" ! active.shutsDownOn(PeerActive) ^
      sendsHearbeat(active, PeerActive) ^ end


  def sendsHearbeat(context: BinaryStarContext, msg: BinaryStarEvent) = {
    "send the appropriate state as heartbeat" ! context.sendsHeartbeat(msg) ^ bt
  }

  def `and a PeerActive is received`(context: BinaryStarContext) = {
    "and a PeerActive is received" ^
      "notify the listener to deactivate" ! context.notifiesListenerToDeactivate ^
      "move to state passive" ! context.goesToPassive(PeerActive) ^ bt
  }

  def primary = new PrimaryBinaryStarContext
  def backup = new BackupBinaryStarContext
  def passive = new PassiveBinaryStarContext
  def active = new ActiveBinaryStarContext

  trait BinaryStarContext extends TestKit with ZeroMqContext {
    import BinaryStar._
    
    def startAs: BinaryStar.BinaryStarState
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

    def sendsHeartbeat(msg: BinaryStarEvent) = this {
      withServer(ZMQ.SUB) { server =>
        val latch = TestLatch()
        server onMessage { frames =>
          Messages(zmqMessage(frames)) match {
            case `msg` => latch.countDown()
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
    
    def shutsDownOn(events: Any*) = this {
      events map { ev =>
        val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
        fsm ! ev
        fsm.isShutdown must beTrue.eventually
      } reduce (_ and _)
    }

  }
  
  class BackupBinaryStarContext extends BinaryStarContext  {
    import BinaryStar._
    import Messages._
    val startAs = BinaryStar.Backup

  }
  class PassiveBinaryStarContext extends BinaryStarContext  {
    import BinaryStar._
    import Messages._
    val startAs = BinaryStar.Passive

    def notifiesOnTransition = {
      val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
      fsm ! PeerPrimary
      receiveOne(2.seconds) must_== Active
    }
  }
  
  class ActiveBinaryStarContext extends BinaryStarContext  {
    import BinaryStar._
    import Messages._
    val startAs = BinaryStar.Active

  }

  class PrimaryBinaryStarContext extends BinaryStarContext {

    import BinaryStar._
    import Messages._
    val startAs = BinaryStar.Primary

    def forwardsRequest = this {
      val fsm = TestFSMRef(new Reactor(defaultConfig.copy(listener = Some(testActor)))).start
      fsm ! ClientRequest(Ping)
      receiveN(2, 2.seconds).last must_== Ping
    }
    
  }
}