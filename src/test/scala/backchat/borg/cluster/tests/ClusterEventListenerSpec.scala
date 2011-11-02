package backchat.borg
package cluster
package tests

import org.specs2._
import specification.After
import akka.actor._
import Actor._
import cluster.ClusterEvents._
import org.multiverse.api.latches.StandardLatch
import java.util.concurrent.{CountDownLatch, TimeUnit}

class ClusterEventListenerSpec extends Specification { def is =

  "A ClusterEventListener should" ^
    "when handling an AddListener message" ^
      "send a connected event to the listener if the cluster is connected" ! context.sendConnectedEvent ^
      "not send a connected event to the listener if the cluster is not connected" ! context.dontSendConnectedOnDisconnected ^ br^
    "when handling a RemoveListener message" ! context.onRemoveListener ^ br^ bt^
    "when handling a Connected message" ^
      "notify listeners" ! pending ^
      "do nothing if already connected" ! pending ^ br^ bt^
    "when handling a NodesChanged message" ^
      "notify listeners" ! pending ^
      "do nothing if not connected" ! pending ^ br^ bt^
    "when handling a Disconnected message" ^
      "disconnects the cluster" ! pending ^
      "notify listeners" ! pending ^
      "do nothing if not connected" ! pending ^ br^
    "when handling a Shutdown message" ! pending ^ end
  
  def context = new ListenerContext()

  object ListenerContext {
    val shortNodes = Set(Node(1, "localhost:31313", false, Set(1, 2)))
    val nodes = shortNodes ++ List(Node(2, "localhost:31314", true, Set(3, 4)),
      Node(3, "localhost:31315", false, Set(5, 6)))
  }
  class ListenerContext {
    import ClusterEventListener._
    import ListenerContext._

    val latch = new StandardLatch

    def newListener(pf: Receive) = actorOf(new Actor { protected def receive = pf orElse { case _ => } }).start()
    
    def sendConnectedEvent = {
      val eventListener = actorOf(new ClusterEventListener).start()
      eventListener ! Connected(nodes)
      var calls = 0
      var current = Set.empty[Node]
      val listener = newListener {
        case ClusterEvents.Connected(n) => {
          current = n
          calls += 1
          latch.open()
        }
      }
      eventListener ? Messages.AddListener(listener)
      latch.tryAwait(2, TimeUnit.SECONDS)
      listener.stop()
      eventListener ! Shutdown
      println(current)
      (calls must_== 1) and (current.size must_== 1) and (current.head.id must_== 2)
    }

    def dontSendConnectedOnDisconnected = {
      val eventListener = actorOf(new ClusterEventListener).start()
      var counted = 0
      val listener = newListener {
        case Connected(_) => {
          counted += 1
          latch.open()
        }
      }
      eventListener ? Messages.AddListener(listener)
      val res = latch.tryAwait(2, TimeUnit.SECONDS)
      listener.stop()
      eventListener ! Shutdown
      res must beFalse and (counted must_== 0)
    }

    def onRemoveListener = {
      val callbackCounter = new CountDownLatch(3)
      val callback = actorOf(new Actor {
        protected def receive = {
          case Connected(_) | Messages.RemoveListener(_) | NodesChanged(_) => callbackCounter.countDown()
        }
      }).start()
      val eventListener = actorOf(new ClusterEventListener(Some(callback))).start()
      var calls = 0
      val listener = newListener {
        case Connected(_) | NodesChanged(_) => calls += 1
      }
      
      val key = (eventListener ? Messages.AddListener(listener)).as[Messages.AddedListener].get.key
      eventListener ! Connected(nodes)
      eventListener ! Messages.RemoveListener(key)
      eventListener ! NodesChanged(nodes)
      callbackCounter.await(2, TimeUnit.SECONDS)
      listener.stop()
      eventListener ! Shutdown
      calls must_== 1
    }


  }
}