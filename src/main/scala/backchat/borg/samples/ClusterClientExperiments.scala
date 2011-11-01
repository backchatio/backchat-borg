package backchat
package borg
package samples

import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.Watcher.Event.KeeperState

//object ClusterClientExperiments extends App with Logging {
//
//  val latch = new CountDownLatch(2)
//  val zk = new ZkClient("localhost:16505", 5000, 5000, new SerializableSerializer)
//  sys.addShutdownHook(zk.close())
//  zk.subscribeStateChanges(new IZkStateListener {
//    def handleStateChanged(p1: KeeperState) {
//      p1 match {
//        case KeeperState.SyncConnected ⇒ {
//          logger info "Connected to zookeeper server"
//        }
//        case e ⇒ logger debug "Unhandled: %s".format(e)
//      }
//    }
//
//    def handleNewSession() {
//      logger debug "new session started"
//    }
//  })
//  //zk.waitUntilConnected()
//  logger debug "should be connected now"
//  zk.createEphemeral("/tester")
//  logger debug "created ephemeral node"
//  latch.await()
//}