package backchat
package zeromq
package tests

import akka.testkit._
import org.scalatest.matchers.MustMatchers
import akka.actor._
import Actor._
import akka.util.duration._
import Messages._
import collection.immutable.Queue
import net.liftweb.json._
import org.zeromq.ZMQ
import org.scalatest.{ BeforeAndAfterAll, WordSpec }

object ServiceRegistrySpec {
  class ServerTestWorker(callback: ActorRef) extends Actor {
    protected def receive = {
      case ApplicationEvent('the_request, JNothing) if self.sender == Some(callback) ⇒ {
        callback ! 'matched
      }
    }
  }

  val context = ZMQ context 1
}
class ServiceRegistrySpec extends WordSpec with MustMatchers with TestKit with BeforeAndAfterAll {

  import ServiceRegistrySpec._
  val ccid = new Uuid()
  val reqMatch: PartialFunction[Any, Boolean] = {
    case m: Request if m.target == "message_channels" && m.event.action == 'the_request ⇒ true
  }
  val appEvtMatch: PartialFunction[Any, Boolean] = {
    case ApplicationEvent('the_request, _) ⇒ true
  }

  "A service registry" when {

    "requesting the registered services" should {
      "eventually return a list of services" in {
        val ser = Map("a" -> Queue("aaa"), "b" -> Queue("bbb"), "c" -> Queue("ccc"))
        val sut = actorOf(new ServiceRegistry(ser)).start()
        within(1.second) {
          sut ! ServiceListRequest(null)
          expectMsg(ServiceList(List("a", "b", "c"), null))
        }
        sut.stop()
      }
    }

    "registering a service" should {
      "create a new set if this is the first worker" in {
        val sut = actorOf(new ServiceRegistry(callback = Some(testActor))).start()
        within(2.seconds) {
          sut ! RegisterService("message_channels", "the-message-channel-actor")
          expectMsg('registered -> Queue("the-message-channel-actor"))
        }
        sut.stop()
      }
      "append to a set if there isn't the first" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue("the-message-channel-actor")), Some(testActor))).start()
        within(2.seconds) {
          sut ! RegisterService("message_channels", "the-second-message-channel-actor")
          expectMsg('registered -> Queue("the-message-channel-actor", "the-second-message-channel-actor"))
        }
        sut.stop()
      }
    }

    "unregistering a service" should {
      "remove the worker from the set if it isn't the last worker to unregister" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue("the-message-channel-actor", "the-second-message-channel-actor")), Some(testActor))).start()
        within(2.seconds) {
          sut ! UnregisterService("message_channels", "the-second-message-channel-actor")
          expectMsg('unregistered -> Some(Queue("the-message-channel-actor")))
        }
        sut.stop()
      }
      "remove the key if this is the last worker to unregister" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue("the-message-channel-actor")), Some(testActor))).start()
        within(1.second) {
          sut ! UnregisterService("message_channels", "the-message-channel-actor")
          expectMsg('unregistered -> None)
        }
        sut.stop()
      }
    }

    "forwarding a request" should {
      "send the request on to the actor" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id)), Some(testActor))).start()
        within(2.seconds) {
          sut ! Request("message_channels", ApplicationEvent('the_request), ccid)
          expectMsgPF()(appEvtMatch)
          expectMsg('forwarded -> Queue(testActor.id))
        }
        sut.stop()
      }
      "retain the original sender of the message" in {
        val worker = actorOf(new ServerTestWorker(testActor)).start()
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(worker.id)), Some(testActor))).start()
        within(2.seconds) {
          sut ! Request("message_channels", ApplicationEvent('the_request), ccid)
          expectMsgAllOf('forwarded -> Queue(worker.id), 'matched)
        }
        sut.stop()
      }
      "move the id of the worker to the back of the set (when this isn't the only one in the set)" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id, "another-actor")), Some(testActor))).start()
        within(2.seconds) {
          sut ! Request("message_channels", ApplicationEvent('the_request), ccid)
          expectMsgPF()(appEvtMatch)
          expectMsg('forwarded -> Queue("another-actor", testActor.id))
        }
        sut.stop()
      }
    }

    "forwarding a fireandforget message" should {
      "send the request on to the actor" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id)), Some(testActor))).start()
        within(2.seconds) {
          sut ! Enqueue("message_channels", ApplicationEvent('the_request), ccid)
          expectMsgPF()(appEvtMatch)
          expectMsg('forwarded -> Queue(testActor.id))
        }
        sut.stop()
      }
      "move the id of the worker to the back of the set" in {
        val sut = actorOf(new ServiceRegistry(Map("message_channels" -> Queue(testActor.id, "another-actor")), Some(testActor))).start()
        within(2.seconds) {
          sut ! Enqueue("message_channels", ApplicationEvent('the_request), ccid)
          expectMsgPF()(appEvtMatch)
          expectMsg('forwarded -> Queue("another-actor", testActor.id))
        }
        sut.stop()
      }
    }
  }

  override protected def afterAll() {
    //    Actor.registry.shutdownAll()
    stopTestActor
  }
}