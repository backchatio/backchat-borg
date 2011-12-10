package backchat
package borg
package cadence

import com.ning.http.client.AsyncHttpClient
import net.liftweb.json._
import mojolly._
import config.{ ConfigurationContext, MojollyConfig }
import metrics.MetricsConfig
import akka.util.Bootable
import Alerter._

/**
 * Polls Graphite and logs when a metric passes a threshold.
 */
class Alerter(config: AlerterContext) extends Bootable with Logging {
  lazy val httpClient = new AsyncHttpClient

  lazy val alertLogger = Logger("ALERTS")

  override def onLoad() {
    super.onLoad()
    logger debug ("polling %s every %d seconds: " format (config.url, config.period))
    Timer(config.period.seconds)(run)
  }

  override def onUnload() {
    super.onUnload()
    httpClient.close()
  }

  def requestUrl(window: Duration) = "%s/render?format=json&from=-%ds" format (config.url, window.seconds)

  def run {
    config.alerts foreach { alert ⇒
      val req = (httpClient prepareGet requestUrl(alert.window))
      req addQueryParameter ("target", alert.target)
      val resp = req execute () get ()
      logger debug ("polling " + resp.getUri)
      val json = resp.getResponseBody
      logger debug ("response:" + json)
      GraphiteResponse(json) foreach handle
    }
  }

  def warn(a: Alert, m: Metric) = alertLogger warn ("warn threshold for %s passed" format a.target)
  def error(a: Alert, m: Metric) = alertLogger error ("error threshold for %s passed" format a.target)

  def handle(resp: GraphiteResponse) = {
    logger debug ("metric: " + resp.metrics)
    resp.metrics foreach { m ⇒
      config.alerts find (_.target == m.target) foreach { a ⇒
        if (m.datapoints.size > 0) {
          val sum = (m.datapoints map (_.value)).sum
          val avg = sum / m.datapoints.size
          logger debug ("avg for %s: %s" format (a.target, avg))
          avg match {
            case e if avg > a.error ⇒ error(a, m)
            case e if avg > a.warn  ⇒ warn(a, m)
            case _                  ⇒
          }
        } else
          logger debug ("no data points for " + a.target)
      }
    }
  }
}

object Alerter {
  class AlerterConfig(key: String = "application") extends MojollyConfig(ConfigurationContext(key)) with MetricsConfig {
    val applicationName = "Alerter"
    val alerter = AlerterContext(
      "http://%s" format (reporting map (_.host) getOrElse "graphite"),
      config.getSection("mojolly.reporting") flatMap (_.getInt("pollInterval")),
      config.getSection("mojolly.borg.cadence.alerts") map { section ⇒
        section.keys map { k ⇒
          val key = k.split("\\.", 2).head
          Alert(
            section.getString(key + ".graphitekeypattern").get,
            section.getDouble(key + ".error").get,
            section.getDouble(key + ".warn").get)
        }
      } getOrElse Nil
    )
  }

  case class AlerterContext(url: String, period: Int, alerts: Seq[Alert])
  case class Alert(target: String, warn: Double, error: Double, window: Duration = 5.minutes)

  case class GraphiteResponse(metrics: List[Metric])
  case class Metric(target: String, datapoints: List[Datapoint])
  case class Datapoint(timestamp: Long, value: Double)

  object GraphiteResponse {
    def apply(jsonString: String): Option[GraphiteResponse] = {
      parse(jsonString) match {
        case JArray(metric) ⇒ Some(GraphiteResponse(metric collect {
          case JObject(JField("target", JString(target)) :: JField("datapoints", JArray(datapoints)) :: Nil) ⇒
            Metric(target, datapoints collect {
              case JArray(List(JDouble(v), JInt(timestamp))) ⇒ Datapoint(timestamp.toLong, v)
            })
        }))
        case _ ⇒ None
      }
    }
  }
}