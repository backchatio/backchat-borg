package backchat
package borg
package hive
package tests

import mojolly.testing.AkkaSpecification
import akka.testkit._
import akka.actor._
import Actor.actorOf
import hive.TrackerRegistry.TrackerNode
import org.apache.zookeeper.CreateMode
import util.Random
import akka.routing.{WithListeners, Listen}

class TrackerRegistrySpec extends ZooKeeperActorSpecification {

  def is =
    "A tracker registry should" ^
      "fill the registry at startup" ! specify.getTheInitialStateForSubtree ^
      "get a key from the registry" ! specify.getsNodeForKey ^
      "set a key in the registry" ! specify.setsKey ^
      "notify listeners of value updated" ! specify.notifiesListeners ^
      "keep the registry in sync with network changes" ! specify.syncsWithNetwork ^
    end

  def specify = new TrackerRegistryContext("/trackers-" + Random.nextInt(300))

  class TrackerRegistryContext(rootNode: String) extends ZooKeeperClientContext(zookeeperServer, rootNode) with TestKit {

    val tracker1 = TrackerNode("tracker-1", "twitter", Seq("timeline"), 1)
    val tracker2 = TrackerNode("tracker-2", "email", Seq("smtp"), 2)
    val tracker3 = TrackerNode("tracker-3", "xmpp", Seq("gtalk"), 3)

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

    def notifiesListeners = {
      awaitInit._2 must haveTheSameElementsAs(Vector(tracker1, tracker2, tracker3))
    }

    private def awaitInit = {
      val probe = TestProbe()
      probe.send(probe.ref, 'hello)
      probe.expectMsg(2.seconds, 'hello)
      val a = actorOf(new TrackerRegistry.TrackerRegistryActor(config)).start()
      a ! Listen(probe.ref)
      sleep -> 50.millis
      a ! WithListeners(_ ! 'ok)
      receiveWhile(2.seconds) {
        case 'ok => true
        case m => println(m)
      }
      setupNodes()
      (probe, probe.receiveN(3))
    }

    def setsKey = {
      awaitInit
      val newnode = TrackerNode("tracker-4", "twitter", Seq("oauth"), 4)
      TrackerRegistry.set(newnode)
      TrackerRegistry.get(newnode.id) must_== Some(newnode)
    }

    def syncsWithNetwork = {
      val (probe, _) = awaitInit
      val newnode = TrackerNode("tracker-4", "twitter", Seq("oauth"), 4)
      zkClient.create(node(newnode.id), newnode.toBytes, CreateMode.EPHEMERAL)
      probe.expectMsg(newnode) must_== newnode
    }
  }
}