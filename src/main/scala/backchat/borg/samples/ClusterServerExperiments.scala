package backchat
package borg
package samples

import collection.mutable
import java.io.File
import org.apache.commons.lang.SystemUtils
import com.eaio.uuid.UUID
import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.net.InetSocketAddress
import org.apache.zookeeper.server.{ ServerCnxnFactory, NIOServerCnxnFactory, ZooKeeperServer }
import com.twitter.zookeeper.ZookeeperClient

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
class ZookeeperRandomPortServer(sessionTimeout: Period = 100.millis) extends Logging {

  private val shutDownActions = mutable.ListBuffer.empty[() ⇒ Unit]
  private var connectionFactory: ServerCnxnFactory = null
  private var started = false
  var port: Int = -1

  private val zookeeperServer = new ZooKeeperServer(new FileTxnSnapLog(createTempDir, createTempDir), new BasicDataTreeBuilder())

  def createTempDir = {
    val tempDir = FileUtils.createTempDir()
    shutDownActions += { () ⇒ org.apache.commons.io.FileUtils.deleteDirectory(tempDir) }
    tempDir
  }

  private def startNetwork() {
    connectionFactory = new NIOServerCnxnFactory()
    connectionFactory.configure(new InetSocketAddress(NetworkUtil.randomFreePort()), 1024)
    connectionFactory.startup(zookeeperServer)

    shutDownActions += { () ⇒ if (connectionFactory.isNotNull) connectionFactory.closeAll() }

    port = zookeeperServer.getClientPort
  }

  def start() = {
    if (!started) {
      startNetwork()
      started = true
      logger info "Zookeeper Server started on [%s]".format(port)
    }
  }

  def restart() {
    if (port < 0) start()
    else {
      startNetwork()
      logger info "Zookeeper Server restarted on [%s]".format(port)
    }
  }

  def stop() = {
    if (started) {
      shutDownActions foreach { _.apply() }
      zookeeperServer.shutdown()
    }
  }

  def newClient(sessionTimeout: Duration = 3.seconds) = {
    require(started, "The server needs to be started to spawn clients")
    val cl = new ZookeeperClient("127.0.0.1:%s".format(port))
    shutDownActions += { () ⇒ cl.close() }
    cl
  }

  def expireClientSession(client: ZookeeperClient) {
    zookeeperServer.closeSession(client.getHandle.getSessionId)
  }

}

object ClusterServerExperiments extends App {

  val latch = new CountDownLatch(2)
  val serv = new ZookeeperRandomPortServer()
  serv.start()
  sys.addShutdownHook(serv.stop())
  latch.await()
}