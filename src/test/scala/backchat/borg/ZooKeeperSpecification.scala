package backchat.borg

import testing.ZooKeeperTestServer
import akka.actor.Actor
import org.specs2.Specification
import com.twitter.zookeeper.{ ZooKeeperClient, ZooKeeperClientConfig }
import org.specs2.specification.{ Step, After, Fragments }
import mojolly.testing.{ MojollySpecification, AkkaSpecification }

trait ZooKeeperActorSpecification extends AkkaSpecification with ZooKeeperSpecification

trait ZooKeeperSpecification extends MojollySpecification {
  val zookeeperServer = new ZooKeeperTestServer()
  override def map(fs: ⇒ Fragments) = Step(zookeeperServer.start()) ^ super.map(fs) ^ Step(zookeeperServer.stop())

  def specify: ZooKeeperClientContext
}

abstract class ZooKeeperClientContext(server: ZooKeeperTestServer, root: String = "") extends After {
  val config = new ZooKeeperClientConfig {
    def hostList = "localhost:%s" format server.port
    override val rootNode = root
  }

  val hostlist = config.hostList

  val zkClient = server.newClient()
  zkClient.connect()
  private var _afters = List[() ⇒ Any]()

  def doAfter(fn: ⇒ Any) {
    _afters ::= (() ⇒ fn)
  }

  def after = {
    _afters foreach (_.apply)
    server.expireClientSession(zkClient)
  }
}