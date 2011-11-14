package backchat
package borg
package hive
package testing

import collection.mutable
import java.io.File
import org.apache.commons.lang.SystemUtils
import com.eaio.uuid.UUID
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ZooKeeperServer, ServerCnxnFactory }
import java.net.InetSocketAddress
import mojolly.io.{FreePort, TempDir}

class NoRandomPortAvailableException extends Exception

class ZooKeeperTestServer(sessionTimeout: Period = 100.millis, maxRetries: Int = 5) extends Logging {
  private val shutDownActions = mutable.ListBuffer.empty[() ⇒ Unit]
  private var connectionFactory: ServerCnxnFactory = null
  private var started = false
  var port: Int = -1

  private val zookeeperServer = new ZooKeeperServer(new FileTxnSnapLog(createTempDir, createTempDir), new BasicDataTreeBuilder())

  def createTempDir = {
    val tempDir = TempDir()
    shutDownActions += { () ⇒ org.apache.commons.io.FileUtils.deleteDirectory(tempDir) }
    tempDir
  }

  private def startNetwork() {
    connectionFactory = new NIOServerCnxnFactory()
    connectionFactory.configure(new InetSocketAddress(FreePort.randomFreePort()), 1024)
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

  //  def newClient(sessionTimeout: Duration = 3.seconds) = {
  //    require(started, "The server needs to be started to spawn clients")
  //    val cl = new ZookeeperClient("127.0.0.1:%s".format(port))
  //    shutDownActions += { () ⇒ cl.close() }
  //    cl
  //  }
  //
  //  def expireClientSession(client: ZookeeperClient) {
  //    zookeeperServer.closeSession(client.getHandle.getSessionId)
  //  }
}
