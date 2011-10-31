package backchat
package borg
package zookeeper
package tests

import collection.mutable
import java.io.File
import org.apache.commons.lang.SystemUtils
import com.eaio.uuid.UUID
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import org.apache.zookeeper.server.ZooKeeperServer.BasicDataTreeBuilder
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, NIOServerCnxn, ZooKeeperServer }
import org.I0Itec.zkclient.{ ZkClient, IDefaultNameSpace, ZkServer }

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
class ZookeeperTestServer(sessionTimeout: Period = 100.millis) {

  private val shutDownActions = mutable.ListBuffer.empty[() ⇒ Unit]
  var port: Int = -1

  private var connectionFactory = new NIOServerCnxnFactory()

  private val zookeeperServer = {
    val serv = new ZkServer(createTempFile, createTempFile, noopNameSpace, 2181, 500)
    serv.start()
    serv
  }

  def noopNameSpace = new IDefaultNameSpace {
    def createDefaultNameSpace(p1: ZkClient) {}
  }

  def createTempFile = {
    val tempDir = FileUtils.createTempDir()
    shutDownActions += { () ⇒ org.apache.commons.io.FileUtils.deleteDirectory(tempDir) }
    tempDir.getAbsolutePath
  }

}