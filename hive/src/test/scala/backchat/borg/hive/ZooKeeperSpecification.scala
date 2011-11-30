package backchat.borg.hive

import testing.ZooKeeperTestServer
import akka.actor.Actor
import org.specs2.Specification
import com.twitter.zookeeper.{ ZooKeeperClient, ZooKeeperClientConfig }
import org.specs2.specification.{ Step, After, Fragments }
import mojolly.testing.{ MojollySpecification, AkkaSpecification }

trait ZooKeeperActorSpecification extends AkkaSpecification {

  val zookeeperServer = new ZooKeeperTestServer()

  private def startZookeeper = zookeeperServer.start()
  private def stopZookeeper = zookeeperServer.stop()

  override def map(fs: ⇒ Fragments) = Step(startZookeeper) ^ super.map(fs) ^ Step(stopZookeeper) ^ Step(Actor.registry.shutdownAll())

}
trait ZooKeeperSpecification extends MojollySpecification {
  val zookeeperServer = new ZooKeeperTestServer()
  override def map(fs: ⇒ Fragments) = Step(zookeeperServer.start()) ^ super.map(fs) ^ Step(zookeeperServer.stop())

  def specify: ZooKeeperClientContext
}

abstract class ZooKeeperClientContext(port: Int) extends After {
  val config = new ZooKeeperClientConfig {
    def hostList = "localhost:%s" format port
  }
  val hostlist = config.hostList

  val zkClient = new ZooKeeperClient(config)
  zkClient.connect()
  private var _afters = List[() ⇒ Any]()

  def doAfter(fn: ⇒ Any) {
    _afters ::= (() ⇒ fn)
  }

  doAfter { zkClient.close() }
  def after = {
    _afters foreach (_.apply)
  }
}