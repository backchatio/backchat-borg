package backchat.borg

import akka.actor._
import Actor._
import akka.config.Supervision._
import cadence.SystemMetricsCollector

object SystemMetricsDaemon extends App with Logging {

  private def sp(key: String) = sys.props get key
  private def ep(key: String) = sys.env get key
  val daemon = sp("backchat.daemon") orElse sp("bc.daemon") orElse ep("BC_DAEMON") orElse ep("BACKCHAT_DAEMON")

  KeepAlive(daemon.isDefined) {
    val factory = SupervisorFactory(
      SupervisorConfig(
        OneForOneStrategy(List(classOf[Exception]), 3, 1000),
        Supervise(actorOf[SystemMetricsCollector], Permanent) :: Nil))

    factory.newInstance.start
    logger info "The backchat system metrics daemon has been started"
  }
}