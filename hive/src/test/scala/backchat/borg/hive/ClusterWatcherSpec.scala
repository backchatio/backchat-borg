package backchat.borg
package hive
package tests

import org.specs2.Specification
import org.apache.zookeeper.Watcher.Event.{KeeperState, EventType}
import org.specs2.mock.Mockito
import akka.actor._
import Actor._
import org.specs2.specification.Around
import org.specs2.execute.Result
import org.apache.zookeeper.{Watcher, ZooKeeper, WatchedEvent}

class ClusterWatcherSpec extends Specification { def is =

  "A ClusterWatcher should" ^
    "send a connected event when Zookeeper connects" ! context.sendsConnected ^
    "send a Disconnected event when Zookeeper disconnects" ! context.sendsDisconnecte ^
    "send an Expired event when Zookeeper's connection expires" ! context.sendsExpired ^
    "send a NodeChildrenChanged event when nodes change" ! context.sendsNodeChildrenChanged ^
    end

  def context = new ClusterWatcherSpecContext
  class ClusterWatcherSpecContext extends ZooKeeperClusterManagerComponent
          with ClusterNotificationManagerComponent with Around {

    import ZooKeeperMessages._
    import ClusterManagerMessages._

    var connectedCount = 0
    var disconnectedCount = 0
    var expiredCount = 0
    var nodesChangedCount = 0
    var nodesChangedPath = ""

    val zkm = actorOf(new Actor {
      protected def receive = {
        case Connected => connectedCount += 1
        case Disconnected => disconnectedCount += 1
        case Expired => expiredCount += 1
        case NodeChildrenChanged(path) => nodesChangedCount += 1; nodesChangedPath = path
      }
    }).start()

    val clusterWatcher = new ClusterWatcher(zkm)

    def newEvent(state: KeeperState) = {
      new WatchedEvent(EventType.None, state, null)
    }
    
    val clusterManager = zkm
  
    val rootNode = "/test"
    val membershipNode = rootNode + "/members"
    val availabilityNode = rootNode + "/available"
  
    val clusterNotificationManager = actorOf(new Actor {
      protected def receive = {
        case _ =>
      }
    }).start()
    
    def sendsConnected = this {
      val event = newEvent(KeeperState.SyncConnected)
      clusterWatcher.process(event)
      connectedCount must eventually(be_==(1))
    }
    
    def sendsDisconnecte = this {
      val event = newEvent(KeeperState.Disconnected)

      clusterWatcher.process(event)

      disconnectedCount must eventually(be_==(1))
    }

    def sendsExpired = this {
      val event = newEvent(KeeperState.Expired)

      clusterWatcher.process(event)

      expiredCount must eventually(be_==(1))

    }

    def sendsNodeChildrenChanged = this {
      val path = "thisIsThePath"
      val event = new WatchedEvent(EventType.NodeChildrenChanged, null, path)

      clusterWatcher.process(event)

      (nodesChangedCount must eventually(be_==(1))) and 
      (nodesChangedPath must be_==(path))
    }

    def around[T](t: => T)(implicit evidence$1: (T) => Result) = {
      clusterManager.start()
      val res = t
      clusterManager ! Shutdown
      res
    }
  }
}