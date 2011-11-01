/*
package backchat.borg
package samples

import java.util.concurrent.CountDownLatch
import org.I0Itec.zkclient.serialize.SerializableSerializer
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.I0Itec.zkclient.{ IZkChildListener, IZkStateListener, ZkClient }
import java.lang.String
import java.util.List
import collection.JavaConversions._

object ClusterReaderExperiments extends App with Logging {
  val latch = new CountDownLatch(2)
  val zk = new ZkClient("localhost:16505", 5000, 5000, new SerializableSerializer)
  sys.addShutdownHook(zk.close())
  zk.subscribeStateChanges(new IZkStateListener {
    def handleStateChanged(p1: KeeperState) {
      p1 match {
        case KeeperState.SyncConnected ⇒ {
          logger info "Connected to zookeeper server"
        }
        case e ⇒ logger info "Unhandled: %s".format(e)
      }
    }

    def handleNewSession() {
      logger info "new session started"
    }
  })

  //  zk.waitUntilConnected()
  logger info "should be connected now"
  logger info "The node /tester exists? %s".format(zk.exists("/tester"))
  if (!zk.exists("/testing")) {
    zk.createPersistent("/testing")
    logger debug "Created the testing node again"
  }

  zk.subscribeChildChanges("/testing", new IZkChildListener {
    def handleChildChange(path: String, extra: List[String]) {
      if (extra != null)
        logger debug "path: %s, extra: %s".format(path, extra.mkString(", "))
      else
        logger debug "removed node from %s".format(path)
    }
  })
  zk.createEphemeral("/testing/anode", "the data")
  zk.deleteRecursive("/testing")
  logger info "created ephemeral node"
  latch.await()
}*/
