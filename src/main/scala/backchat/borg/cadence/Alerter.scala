package backchat
package borg
package cadence

import com.ning.http.client.AsyncHttpClient
import java.util.concurrent.TimeUnit
import net.liftweb.json._
import mojolly._
import config.{ ConfigurationContext, MojollyConfig }
import metrics.MetricsConfig
import akka.actor.Scheduler
import akka.util.Bootable
import Alerter._

/**
 * Polls Graphite and logs when a metric passes a threshold.
 */
class Alerter(config: AlerterConfig) extends Bootable with Logging {
  lazy val httpClient = new AsyncHttpClient

  lazy val alertLogger = Logger("ALERTS")

  override def onLoad() {
    super.onLoad()
    logger debug ("polling %s every %d seconds: " format (config.url, config.period))
    Scheduler schedule (() ⇒ run, 0, config.period, TimeUnit.SECONDS)
  }

  override def onUnload() {
    super.onUnload()
    httpClient.close()
  }

  def requestUrl = "%s/render?format=json&from=-%ds" format (config.url, config.period)

  def run {
    val req = (httpClient prepareGet requestUrl)
    config.alerts foreach { a ⇒
      req addQueryParameter ("target", a.target)
    }
    val resp = req execute () get ()
    logger debug ("polling " + resp.getUri)
    val json = resp.getResponseBody
    logger debug ("response:" + json)
    GraphiteResponse(json) foreach handle
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
    val alerts = config.getSection("mojolly.borg.cadence.alerts") map { section ⇒
      section.keys.map(_.split("\\.", 2)).map(_.head) map { k ⇒
        Alert(
          target = section.getString(k + ".graphitekeypattern").get,
          warn = section.getDouble(k + ".error").get,
          error = section.getDouble(k + ".warn").get)
      }
    } getOrElse Nil
    val url = "http://%s" format (reporting map (_.host) getOrElse "graphite")
    val period = config.getSection("mojolly.reporting") flatMap { sect ⇒
      sect.getInt("pollInterval")
    } getOrElse 60
  }

  case class Alert(target: String, warn: Double, error: Double)

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