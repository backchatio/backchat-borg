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

class TrackerRegistrySpec extends ZooKeeperActorSpecification {

  def is = 
    "A tracker registry should" ^
      "fill the registry at startup" ! specify.getTheInitialStateForSubtree ^
      "get a key from the registry" ! pending ^
      "set a key in the registry" ! pending ^
      "notify listeners of value updated" ! pending ^
      "keep the registry in sync with network changes" ! pending ^
    end

  def specify = new ActorContext

  class ActorContext extends ZooKeeperClientContext(zookeeperServer) with TestKit {

    val tracker1 = TrackerNode("tracker-1", "twitter", Seq("timeline"), 1)
    val tracker2 = TrackerNode("tracker-2", "email", Seq("smtp"), 2)
    val tracker3 = TrackerNode("tracker-3", "xmpp", Seq("gtalk"), 3)
    
    def getTheInitialStateForSubtree = {
      zkClient.createPath("/trackers")
      zkClient.create("/trackers/tracker-1", tracker1.toBytes, CreateMode.EPHEMERAL_SEQUENTIAL)
      zkClient.create("/trackers/tracker-2", tracker2.toBytes, CreateMode.EPHEMERAL_SEQUENTIAL)
      zkClient.create("/trackers/tracker-3", tracker3.toBytes, CreateMode.EPHEMERAL_SEQUENTIAL)
      
      val actor = TestActorRef(new TrackerRegistry.TrackerRegistryActor(config)).start()
      actor.underlyingActor.data.size must be_==(3).eventually
    }
  }
}