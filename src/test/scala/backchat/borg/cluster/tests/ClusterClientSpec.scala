package backchat.borg
package cluster
package tests

import org.specs2.Specification
import akka.actor._
import Actor._
import java.util.concurrent.TimeUnit
import org.multiverse.api.latches.StandardLatch

class ClusterClientSpec extends Specification { def is =

  "A ClusterClient should" ^
    "when starting the cluster notification and zookeeper manager actors" ! context.startsSuccessfully ^
    "start disconnected" ! context.startDisconnected ^
    "start not shutdown" ! context.notShutdown ^
    "throw a ClusterNotStartedException if the cluster wasn't started" ^
      "when nodes is called" ! context.throwsNotStartedFor(_.nodes) ^
      "when nodeWithId is called" ! context.throwsNotStartedFor(_.nodeWithId(1)) ^
      "when addNode is called" ! context.throwsNotStartedFor(_.addNode(1, null, Set(0))) ^
      "when removeNode is called" ! context.throwsNotStartedFor(_.removeNode(1)) ^
      "when markNodeAvailable(1 is called" ! context.throwsNotStartedFor(_.markNodeAvailable(1)) ^
      "when markNodeUnavailable(1 is called" ! context.throwsNotStartedFor(_.markNodeUnavailable(1)) ^
      "when addListener is called" ! context.throwsNotStartedFor(_.addListener(null)) ^
      "when removeListener is called" ! context.throwsNotStartedFor(_.removeListener(null)) ^
      "when awaitConnection is called" ! context.throwsNotStartedFor(_.awaitConnection()) ^
      "when awaitConnection with timeout is called" ! context.throwsNotStartedFor(_.awaitConnection(1, TimeUnit.SECONDS)) ^
      "when awaitConnectionUninterruptibly is called" ! context.throwsNotStartedFor(_.awaitConnectionUninterruptibly()) ^
      "when isConnected is called" ! context.throwsNotStartedFor(_.isConnected) ^
      "when isShutdown is called" ! context.throwsNotStartedFor(_.isShutdown) ^ bt^
    "throw a ClusterShutDownException if shutdown" ^
      "when start is called" ! context.throwsShutDownFor(_.start()) ^
      "when nodes is called" ! context.throwsShutDownFor(_.nodes) ^
      "when nodeWithId is called" ! context.throwsShutDownFor(_.nodeWithId(1)) ^
      "when addListener is called" ! context.throwsShutDownFor(_.addListener(null)) ^
      "when removeListener is called" ! context.throwsShutDownFor(_.removeListener(null)) ^
      "when awaitConnection is called" ! context.throwsShutDownFor(_.awaitConnection()) ^
      "when awaitConnection with timeout is called" ! context.throwsShutDownFor(_.awaitConnection(1, TimeUnit.SECONDS)) ^
      "when awaitConnectionUninterruptibly is called" ! context.throwsShutDownFor(_.awaitConnectionUninterruptibly()) ^ bt^
    "throw a ClusterDisconnectedException if disconnected" ^
      "when nodes is called" ! context.throwsDisconnectedFor(_.nodes) ^
      "when nodeWithId is called" ! context.throwsDisconnectedFor(_.nodeWithId(1)) ^
      "when addNode is called" ! context.throwsDisconnectedFor(_.addNode(1, null, Set(0))) ^
      "when removeNode is called" ! context.throwsDisconnectedFor(_.removeNode(1)) ^
      "when markNodeAvailable(1 is called" ! context.throwsDisconnectedFor(_.markNodeAvailable(1)) ^
      "when markNodeUnavailable(1 is called" ! context.throwsDisconnectedFor(_.markNodeUnavailable(1)) ^ bt^
    "handle a connected event" ! context.handlesConnected ^
    "handle a disconnected event" ! context.handlesDisconnected ^
    "addNode should add a node to the ClusterManager" ! context.addsNode ^
    "removeNode should remove a node to the ClusterManager" ! context.removesNode ^
    "markAvailable marks a node available in the ClusterManager" ! context.marksAvailable ^
    "markUnavailable marks a node unavailable in the ClusterManager" ! context.marksUnvailable ^
    "ask the ClusterManager for the current node list" ! context.asksForCurrentNodes ^
    "when handling nodeWithId" ^
      "return the node that matches the specified id" ! context.returnsNodeMatchingId ^
      "return None if no matching id" ! context.returnsNoneIfNoNodeMatches ^ bt^
    "send AddListener to the NotificationManager for addListener" ! context.sendsAddListener ^
    "send RemoveListener to the NotificationManager for removeListener" ! context.sendsRemoveListener ^
    "shut managers down on shutdown" ! context.shutManagersDown ^
    "handle a listener throwing an exception" ! context.handlesListenerExceptions ^
    end

  def context = new ClientContext
  class ClientContext {
    val clusterListenerKey = ClusterListenerKey(10101L)
    val currentNodes = Set(Node(1, "localhost:31313", true, Set(0, 1)),
           Node(2, "localhost:31314", true, Set(0, 1)),
           Node(3, "localhost:31315", true, Set(0, 1)))
    var clusterActor: ActorRef = _
    var getCurrentNodesCount = 0
    var addListenerCount = 0
    var currentListeners: List[ActorRef] = Nil
    var removeListenerCount = 0
    var cnmShutdownCount = 0

    var addNodeCount = 0
    var nodeAdded: Node = _
    var removeNodeCount = 0
    var nodeRemovedId = 0
    var markNodeAvailableCount = 0
    var markNodeAvailableId = 0
    var markNodeUnavailableCount = 0
    var markNodeUnavailableId = 0
    var zkmShutdownCount = 0

    
    def startsSuccessfully = {
      val c = new ClusterClient with ClusterManagerComponent with ClusterNotificationManagerComponent {
        val clusterNotificationManager = actorOf(new Actor {
          protected def receive = {
            case ClusterNotificationMessages.AddListener(_) => {
              self reply ClusterNotificationMessages.AddedListener(null)
            }
          }
        })
        val clusterManager = actorOf(new Actor {
          protected def receive = {
            case _ =>
          }
        })
        def serviceName = "starttest"
      }
      c.start()
      (c.clusterManager.isRunning must beTrue) and (c.clusterNotificationManager.isRunning must beTrue)
    }

    def startDisconnected = cluster.isConnected must beFalse

    def notShutdown = cluster.isShutdown must beFalse

    def handlesConnected = {
      clusterActor ! ClusterEvents.Connected(Set())
      cluster.isConnected must eventually(beTrue)
    }

    def handlesDisconnected = {
      clusterActor ! ClusterEvents.Connected(Set())
      cluster.isConnected must eventually(beTrue)
      clusterActor ! ClusterEvents.Disconnected
      cluster.isConnected must eventually(beFalse)
    }

    def addsNode = {
      clusterActor ! ClusterEvents.Connected(Set())
      Thread.sleep(10)

      (cluster.addNode(1, "localhost:31313", Set(1, 2)) must not beNull) and
      (addNodeCount must be_==(1)) and
      (nodeAdded.id must be_==(1)) and
      (nodeAdded.url must be_==("localhost:31313")) and
      (nodeAdded.available must be_==(false))
    }
    
    def removesNode = {
      clusterActor ! ClusterEvents.Connected(Set())
      Thread.sleep(10)

      cluster.removeNode(1)
      removeNodeCount must be_==(1) and (nodeRemovedId must be_==(1))
    }

    def marksAvailable = {
      clusterActor ! ClusterEvents.Connected(Set())
      Thread.sleep(10)

      cluster.markNodeAvailable(11)
      markNodeAvailableCount must be_==(1) and (markNodeAvailableId must be_==(11))

    }

    def marksUnvailable = {
      clusterActor ! ClusterEvents.Connected(Set())
      Thread.sleep(10)

      cluster.markNodeUnavailable(111)
      markNodeUnavailableCount must be_==(1) and (markNodeUnavailableId must be_==(111))
    }

    def asksForCurrentNodes = {
      clusterActor ! ClusterEvents.Connected(Set())
      Thread.sleep(10)

      val nodes = cluster.nodes
      nodes.size must be_==(3) and
      (nodes must haveTheSameElementsAs(currentNodes)) and
      (getCurrentNodesCount must be_==(1))
    }

    def returnsNodeMatchingId = {
      clusterActor ! ClusterEvents.Connected(currentNodes)
      Thread.sleep(10L)
      cluster.nodeWithId(2) must beSome[Node].which(currentNodes must contain(_))
    }

    def returnsNoneIfNoNodeMatches = {
      clusterActor ! ClusterEvents.Connected(currentNodes)
      Thread.sleep(10L)
      cluster.nodeWithId(4) must beNone
    }

    def sendsAddListener = {
      val listener = new ClusterListener {
        var callCount = 0
        def handleClusterEvent(event: ClusterEvent): Unit = callCount += 1
      }

      val part1 = (cluster.addListener(listener) must not beNull) and (addListenerCount must be_==(1))
      currentListeners.head ! ClusterEvents.Disconnected
      part1 and (listener.callCount must eventually(be_==(1)))
    }

    def sendsRemoveListener = {
      cluster.removeListener(ClusterListenerKey(1L))
      removeListenerCount must eventually(be_==(1))
    }

    def shutManagersDown = {
      cluster.shutdown

      cnmShutdownCount must eventually(be_==(1))
      zkmShutdownCount must be_==(1)

      cluster.isShutdown must beTrue
    }
    
    def handlesListenerExceptions = {
      val listener = new ClusterListener {
        var callCount = 0
        def handleClusterEvent(event: ClusterEvent) = {
          callCount += 1
          throw new Exception
        }
      }

      val part1 = (cluster.addListener(listener) must not beNull) and (addListenerCount must be_==(1))

      currentListeners.head ! ClusterEvents.NodesChanged(Set())
      currentListeners.head ! ClusterEvents.NodesChanged(Set())

      part1 and (listener.callCount must eventually(be_==(2)))
    }

    def throwsNotStartedFor[T](fn: ClusterClient => T) = {
      val c = new ClusterClient with ClusterManagerComponent with ClusterNotificationManagerComponent {
        def clusterNotificationManager = null
        def clusterManager = null
        def serviceName = null
      }
      fn(c) must throwA[ClusterNotStartedException]
    }

    def throwsShutDownFor[T](fn: ClusterClient => T) = {
      cluster.shutdown()
      fn(cluster) must throwA[ClusterShutdownException]
    }
    def throwsDisconnectedFor[T](fn: ClusterClient => T) = fn(cluster) must throwA[ClusterDisconnectedException]

    val startedLatch = new StandardLatch
    val cluster = new ClusterClient with ClusterManagerComponent with ClusterNotificationManagerComponent {
      def serviceName = "test"

      val clusterNotificationManager = actorOf(new Actor {
        protected def receive = {
          case ClusterNotificationMessages.Connected(nodes) =>
          case ClusterNotificationMessages.AddListener(a) =>
            startedLatch.open()
            if (clusterActor == null) clusterActor = a
            else {
              addListenerCount += 1
              currentListeners = a :: currentListeners
            }
            self reply ClusterNotificationMessages.AddedListener(clusterListenerKey)

          case ClusterNotificationMessages.RemoveListener(key) => removeListenerCount += 1

          case ClusterNotificationMessages.GetCurrentNodes =>
            getCurrentNodesCount += 1
            self reply ClusterNotificationMessages.CurrentNodes(currentNodes)

          case ClusterNotificationMessages.Shutdown =>
            cnmShutdownCount += 1
            self.stop()

        }
      })

      val clusterManager = actorOf(new Actor {
        protected def receive = {
          case ClusterManagerMessages.AddNode(node) =>
            addNodeCount += 1
            nodeAdded = node
            self.reply(ClusterManagerMessages.ClusterManagerResponse(None))

          case ClusterManagerMessages.RemoveNode(id) =>
            removeNodeCount += 1
            nodeRemovedId = id
            self.reply(ClusterManagerMessages.ClusterManagerResponse(None))

          case ClusterManagerMessages.MarkNodeAvailable(id) =>
            markNodeAvailableCount += 1
            markNodeAvailableId = id
            self.reply(ClusterManagerMessages.ClusterManagerResponse(None))

          case ClusterManagerMessages.MarkNodeUnavailable(id) =>
            markNodeUnavailableCount += 1
            markNodeUnavailableId = id
            self.reply(ClusterManagerMessages.ClusterManagerResponse(None))

          case ClusterManagerMessages.Shutdown => {
            zkmShutdownCount += 1
            self.stop()
          }

        }
      })
    }
    cluster.start()
  }
}