package backchat.borg
package cluster
package tests

import org.specs2._
import execute.Result
import akka.actor._
import Actor._
import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.multiverse.api.latches.StandardLatch

class ClusterNotificationManagerSpec extends Specification { def is =

  "A ClusterNotificationManager should" ^
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
  class ListenerContext extends ClusterNotificationManagerComponent {
    import ClusterNotificationMessages._
    import ListenerContext._

    def clusterNotificationManager = null

    def withEventListener(fn: (ActorRef, StandardLatch) => Result) = {
      val act = actorOf(new ClusterNotificationManager).start()
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
      val act = actorOf(new ClusterNotificationManager(Some(callback))).start()
      val res = fn(act, callbackCounter)
      if(act.isRunning) act ! Shutdown
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
        case ClusterEvents.Connected(n) => {
          current = n
          calls += 1
          latch.open()
        }
      }) { listener =>
        eventListener ? AddListener(listener)
        latch.tryAwait(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def dontSendConnectedOnDisconnected = withEventListener { (eventListener, latch) =>
      var calls = 0
      withListener({
        case ClusterEvents.Connected(_) => {
          calls += 1
          latch.open()
        }
      }) { listener => 
        eventListener ? AddListener(listener)
        val res = latch.tryAwait(2, TimeUnit.SECONDS)
        res must beFalse and (calls must_== 0)
      }
      
    }

    def onRemoveListener = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      withListener({ case ClusterEvents.Connected(_) | ClusterEvents.NodesChanged(_) => calls += 1 }) { listener =>
        val key = (eventListener ? AddListener(listener)).as[AddedListener].get.key
        eventListener ! Connected(nodes)
        eventListener ! RemoveListener(key)
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
        eventListener ! AddListener(listener)
        eventListener ! Connected(nodes)
        latch.tryAwait(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def notifyOnlyOnceOnConnected = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      var current = Set.empty[Node]
      withListener({
        case ClusterEvents.Connected(n) => {
          current = n
          calls += 1
        }
      }){ listener =>
        eventListener ! AddListener(listener)
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
        case ClusterEvents.NodesChanged(n) => {
          calls += 1 
          current = n
        }
      }){ listener =>
        eventListener ! Connected(nodes)
        eventListener ! AddListener(listener)
        eventListener ! NodesChanged(nodes)
        callbackCounter.await(2, TimeUnit.SECONDS)
        (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
      }
    }

    def dontNotifyOfChangesIfDisconnected = withEventListener { (eventListener, latch) =>
      var calls = 0
      withListener({
        case ClusterEvents.Connected => calls += 1
        case ClusterEvents.NodesChanged(_) => {
          calls += 1
          latch.open()
        }
      }) { listener =>
        eventListener ! Connected(nodes)
        eventListener ! AddListener(listener)
        eventListener ! NodesChanged(nodes)
        latch.tryAwait(2, TimeUnit.SECONDS) must beTrue and (calls must_== 1)
      }
    }

    def disconnectsCluster = withEventListener { (eventListener, _) =>
      eventListener ! Connected(nodes)
      eventListener ! Disconnected
      (eventListener ask GetCurrentNodes).as[CurrentNodes].get.nodes.size must_== 0
    }

    def notifyOnDisconnect = withEventListenerWithCallback(3) { (eventListener, callbackCounter) =>
      var calls = 0
      withListener({
        case ClusterEvents.Disconnected => calls += 1
      }){ listener =>
        eventListener ! Connected(nodes)
        eventListener ! AddListener(listener)
        eventListener ! Disconnected
        callbackCounter.await(2, TimeUnit.SECONDS)
        calls must_== 1
      }
    }

    def dontNotifyOfDisconnectedIfDisconnected = withEventListenerWithCallback(2) { (eventListener, callbackCounter) =>
      var calls = 0
      withListener({
        case ClusterEvents.Disconnected => calls += 1
      }){ listener =>
        eventListener ! AddListener(listener)
        eventListener ! Disconnected
        callbackCounter.await(2, TimeUnit.SECONDS)
        calls must_== 0
      }
    }

    def shutdownProperly = withEventListenerWithCallback(4) { (eventListener, callbackCounter) =>
      var connectedCalls = 0
      var disconnectedCalls = 0
      withListener({
        case ClusterEvents.Connected(_) => connectedCalls += 1
        case ClusterEvents.Shutdown => disconnectedCalls += 1
      }){ listener =>
        eventListener ! AddListener(listener)
        eventListener ! Connected(nodes)
        eventListener ! Shutdown
        eventListener ! Connected(nodes)
        callbackCounter.await(2, TimeUnit.SECONDS)
        println("Connected: %s, disconnected: %s".format(connectedCalls, disconnectedCalls))
        (connectedCalls must eventually(be_==(1))) and (disconnectedCalls must eventually(be_==(1)))
      }
    }

  }
}