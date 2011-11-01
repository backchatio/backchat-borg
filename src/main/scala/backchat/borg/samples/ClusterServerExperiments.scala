package backchat
package borg
package samples

import collection.mutable
import java.io.File
import org.apache.commons.lang.SystemUtils
import com.eaio.uuid.UUID
import java.util.concurrent.CountDownLatch
import util.Random
import org.I0Itec.zkclient.{ NetworkUtil, ZkClient, IDefaultNameSpace, ZkServer }

object FileUtils {

  private val MAX_TMP_DIR_TRIES = 5
  /**
   * Returns a new empty temporary directory.
   */
  def createTempDir(): File = {
    var tempDir: File = null
    var tries = 0
    do {
      // For sanity sake, die eventually if we keep failing to pick a new unique directory name.
      tries += 1
      if (tries > MAX_TMP_DIR_TRIES) {
        throw new IllegalStateException("Failed to create a new temp directory in "
          + MAX_TMP_DIR_TRIES + " attempts, giving up");
      }
      tempDir = new File(SystemUtils.getJavaIoTmpDir, new UUID().toString);
    } while (!tempDir.mkdir());
    tempDir
  }
}
class ZookeeperRandomPortServer(sessionTimeout: Period = 100.millis, maxRetries: Int = 5) extends Logging {

  private val shutDownActions = mutable.ListBuffer.empty[() ⇒ Unit]
  private var started = false
  var port: Int = -1

  private def newPort = Random.nextInt(55365) + 10000

  private def newZookeeperServer(zkPort: Int) =
    new ZkServer(createTempFile, createTempFile, noopNameSpace, zkPort, 500)

  private val zookeeperServer: ZkServer = {
    var count = 0
    var zkPort = newPort
    while (!NetworkUtil.isPortFree(zkPort)) {
      zkPort = newPort
      count += 1
      if (count >= maxRetries) {
        sys.error("Couldn't determine a free port")
      }
    }
    port = zkPort
    newZookeeperServer(zkPort)
  }

  def noopNameSpace = new IDefaultNameSpace {
    def createDefaultNameSpace(p1: ZkClient) {}
  }

  def createTempFile = {
    val tempDir = FileUtils.createTempDir()
    shutDownActions += { () ⇒ org.apache.commons.io.FileUtils.deleteDirectory(tempDir) }
    tempDir.getAbsolutePath
  }

  def start() = {
    if (!started) {
      zookeeperServer.start()
      started = true
      logger info "Zookeeper Server started on [%s]".format(port)
    }
  }

  def stop() = {
    if (started) {
      zookeeperServer.shutdown()
      shutDownActions foreach { _.apply() }
    }
  }

}

object ClusterServerExperiments extends App {

  val latch = new CountDownLatch(2)
  val serv = new ZookeeperRandomPortServer()
  serv.start()
  sys.addShutdownHook(serv.stop())
  latch.await()
}