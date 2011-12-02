package backchat
package borg
package hive
package tests

import akka.testkit._
import akka.actor._
import Actor.actorOf
import hive.TrackerRegistry._
import org.apache.zookeeper.CreateMode
import util.Random
import java.util.concurrent.{CountDownLatch, TimeUnit}

class TrackerRegistrySpec extends ZooKeeperActorSpecification {

  def is =
    "A tracker registry should" ^
      "fill the registry at startup" ! specify.getTheInitialStateForSubtree ^
      "get a key from the registry" ! specify.getsNodeForKey ^
      "set a key in the registry" ! specify.setsKey ^
      "updates a key in the registry" ! specify.updatesKey ^
      "notify listeners of value updated" ! specify.notifiesListeners ^
      "keep the registry in sync with network changes" ! specify.syncsWithNetwork ^
    end

  def specify = new TrackerRegistryContext("/trackers-" + Random.nextInt(300))

  class TrackerRegistryContext(rootNode: String) extends ZooKeeperClientContext(zookeeperServer, rootNode) with TestKit {

    val tracker1 = TrackerNode("tracker-1", "twitter", Seq("timeline"), 1)
    val tracker2 = TrackerNode("tracker-2", "email", Seq("smtp"), 2)
    val tracker3 = TrackerNode("tracker-3", "xmpp", Seq("gtalk"), 3)
    val tracker4 = TrackerNode("tracker-4", "twitter", Seq("oauth"), 4)
    val tracker5 = TrackerNode("tracker-5", "twitter", Seq("oauth"), 5)
    val tracker6 = TrackerNode("tracker-3", "email", Seq("smtp"), 3)

    doAfter {
      zkClient.deleteRecursive(rootNode)
    }
    
    private def node(path: String) = {
      val pth = if (path.startsWith("/")) path else "/" + path
      rootNode + pth
    }
    
    private def setupNodes() = {
      zkClient.createPath(rootNode)
      zkClient.create(node("tracker-1"), tracker1.toBytes, CreateMode.EPHEMERAL)
      zkClient.create(node("tracker-2"), tracker2.toBytes, CreateMode.EPHEMERAL)
      zkClient.create(node("tracker-3"), tracker3.toBytes, CreateMode.EPHEMERAL)
    }
    
    def getTheInitialStateForSubtree = {
      setupNodes()
      
      val actor = TestActorRef(new TrackerRegistry.TrackerRegistryActor(config)).start()
      actor.underlyingActor.data.size must be_==(3).eventually
    }

    def getsNodeForKey = {
      awaitInit
      TrackerRegistry.get(tracker2.id) must beSome(tracker2)
    }

    def notifiesListeners = awaitInit
    
    var lastNode: TrackerNode = null
    private def awaitInit = {
      lastNode = null
      val started = new CountDownLatch(1)
      val startingLatch = new CountDownLatch(3)
      val l = actorOf(new Actor {
        protected def receive = {
          case 'initialized => started.countDown()
          case m: Messages.TrackerAdded if startingLatch.getCount > 0 => startingLatch.countDown()
          case m: Messages.TrackerAdded => lastNode = m.node
          case _: Messages.TrackerUpdated | _: Messages.TrackerRemoved =>
        }
      }).start()
      actorOf(new TrackerRegistry.TrackerRegistryActor(config, Some(l))).start()
      (started.await(2, TimeUnit.SECONDS) must beTrue) and ({
        setupNodes()
        startingLatch.await(2, TimeUnit.SECONDS) must beTrue
      })
    }

    def setsKey = {
      awaitInit
      TrackerRegistry.set(tracker4)
      TrackerRegistry.get(tracker4.id) must be_==(Some(tracker4)).eventually
    }
    
    def updatesKey = {
      awaitInit
      TrackerRegistry.set(tracker6)
      TrackerRegistry.get(tracker3.id) must be_==(Some(tracker6)).eventually
    }

    def syncsWithNetwork = {
      awaitInit
      zkClient.create(node(tracker5.id), tracker5.toBytes, CreateMode.EPHEMERAL)
      lastNode must be_==(tracker5).eventually
    }
  }
}