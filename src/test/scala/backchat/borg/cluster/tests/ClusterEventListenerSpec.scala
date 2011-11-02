package backchat.borg
package cluster
package tests

import org.specs2._
import execute.Result
import akka.actor._
import Actor._
import cluster.ClusterEvents._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.multiverse.api.latches.StandardLatch

class ClusterEventListenerSpec extends Specification { def is =

  "A ClusterEventListener should" ^
    "when handling an AddListener message" ^
      "send a connected event to the listener if the cluster is connected" ! context.sendConnectedEvent ^
      "not send a connected event to the listener if the cluster is not connected" ! context.dontSendConnectedOnDisconnected ^ br^
    "when handling a RemoveListener message" ! context.onRemoveListener ^ br^ bt^
    "when handling a Connected message" ^
      "notify listeners" ! context.notifyOnConnected ^
      "do nothing if already connected" ! context.notifyOnlyOnceOnConnected ^ br^ bt^
    "when handling a NodesChanged message" ^
      "notify listeners" ! context.notifyOfNodeChanged ^
      "do nothing if not connected" ! context.dontNotifyOfChangesIfDisconnected ^ br^ bt^
    "when handling a Disconnected message" ^
      "disconnects the cluster" ! context.disconnectsCluster ^
      "notify listeners" ! context.notifyOnDisconnect ^
      "do nothing if not connected" ! context.dontNotifyOfDisconnectedIfDisconnected ^ br^
    "when handling a Shutdown message" ! context.shutdownProperly ^ end
  
  def context = new ListenerContext()

  object ListenerContext {
    val shortNodes = Set(Node(1, "localhost:31313", false, Set(1, 2)))
    val nodes = shortNodes ++ List(Node(2, "localhost:31314", true, Set(3, 4)),
      Node(3, "localhost:31315", false, Set(5, 6)))
  }
  class ListenerContext {
    import ClusterEventListener._
    import ListenerContext._

    def withEventListener(fn: (ActorRef, StandardLatch) => Result) = {
      val act = actorOf(new ClusterEventListener).start()
      val res = fn(act, new StandardLatch)
      act ! Shutdown
      res
    }
    def withEventListenerWithCallback(amount: Int)(fn: (ActorRef, CountDownLatch) => Result) = {
      val callbackCounter = new CountDownLatch(3)
      val callback = actorOf(new Actor {
        protected def receive = {
          case Shutdown => self.stop()
          case _ => callbackCounter.countDown()
        }
      }).start()
      val act = actorOf(new ClusterEventListener(Some(callback))).start()
      val res = fn(act, callbackCounter)
      act ! Shutdown
      res
    }
    def newListener(pf: Receive) = actorOf(new Actor { protected def receive = pf orElse { case _ => } }).start()
    def withListener(pf: Receive)(fn: ActorRef => Result) = {
      val act = newListener(pf)
      val res = fn(act)
      act.stop()
      res
    }

    def sendConnectedEvent = withEventListener { (eventListener, latch) =>
      eventListener ! Connected(nodes)
      var calls = 0
      var current = Set.empty[Node]
      withListener({
        case Connected(n) => {
          current = n
          calls += 1
          latch.open()
        }
      }) { listener =>
        eventListener ? Messages.AddListener(listener)
        latch.tryAwait(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def dontSendConnectedOnDisconnected = withEventListener { (eventListener, latch) =>
      var calls = 0
      withListener({
        case Connected(_) => {
          calls += 1
          latch.open()
        }
      }) { listener => 
        eventListener ? Messages.AddListener(listener)
        val res = latch.tryAwait(2, TimeUnit.SECONDS)
        res must beFalse and (calls must_== 0)
      }
      
    }

    def onRemoveListener = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      withListener({ case Connected(_) | NodesChanged(_) => calls += 1 }) { listener => 
        val key = (eventListener ? Messages.AddListener(listener)).as[Messages.AddedListener].get.key
        eventListener ! Connected(nodes)
        eventListener ! Messages.RemoveListener(key)
        eventListener ! NodesChanged(nodes)
        callbackCounter.await(2, TimeUnit.SECONDS)
        calls must_== 1
      }
    }
    
    def notifyOnConnected = withEventListener { (eventListener, latch) =>
      var calls = 0
      var current = Set.empty[Node]
      withListener({
        case ClusterEvents.Connected(n) => {
          current = n
          calls += 1
          latch.open()
        }
      }) { listener => 
        eventListener ! Messages.AddListener(listener)
        eventListener ! Connected(nodes)
        latch.tryAwait(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def notifyOnlyOnceOnConnected = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      var current = Set.empty[Node]
      withListener({
        case Connected(n) => {
          current = n
          calls += 1
        }
      }){ listener =>
        eventListener ! Messages.AddListener(listener)
        eventListener ! Connected(nodes)
        eventListener ! Connected(nodes)
        callbackCounter.await(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def notifyOfNodeChanged = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      var current = Set.empty[Node]
      withListener({
        case NodesChanged(n) => {
          calls += 1 
          current = n
        }
      }){ listener =>
        eventListener ! Connected(nodes)
        eventListener ! Messages.AddListener(listener)
        eventListener ! NodesChanged(nodes)
        callbackCounter.await(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def dontNotifyOfChangesIfDisconnected = withEventListener { (eventListener, latch) =>
      var calls = 0
      withListener({
        case Connected => calls += 1
        case NodesChanged(_) => {
          calls += 1
          latch.open()
        }
      }) { listener =>
        eventListener ! Connected(nodes)
        eventListener ! Messages.AddListener(listener)
        eventListener ! NodesChanged(nodes)
        latch.tryAwait(2, TimeUnit.SECONDS) must beTrue and (calls must_== 1)
      }
    }

    def disconnectsCluster = pending

    def notifyOnDisconnect = pending

    def dontNotifyOfDisconnectedIfDisconnected = pending

    def shutdownProperly = pending

  }
}