package backchat
package borg
package cadence

import com.ning.http.client.AsyncHttpClient
import java.util.concurrent.TimeUnit
import net.liftweb.json._
import mojolly._
import metrics.MetricsConfig
import akka.actor.Scheduler
import akka.util.Bootable
import Alerter._

/**
 * Polls Graphite and logs.
 *
 * TODO SDB:
 * - cleanup config + catch exceptions
 */
class Alerter(config: AlerterConfig) extends Bootable {
  lazy val httpClient = new AsyncHttpClient

  override def onLoad() {
    super.onLoad()
    Scheduler schedule(() => run, 0, config.period, TimeUnit.SECONDS)
  }

  override def onUnload() {
    super.onUnload()
    httpClient.close()
  }

  def requestUrl = "%s/render?format=json&from=-%ds" format (config.url, config.period)

  def run {
    val req = (httpClient prepareGet requestUrl)
    config.alerts foreach { a =>
      req addQueryParameter("target", a.target)
    }
    val resp = req execute() get()
    val json = resp.getResponseBody
    GraphiteResponse(json) foreach handle
  }

  def handle(resp:  GraphiteResponse) = resp.metrics foreach { m =>
    config.alerts find (_.target == m.target) foreach { a =>
      val sum = (m.datapoints map (_.value)).sum
      println(a.target + ": " + sum)
    }
  }
}

object Alerter {
  class AlerterConfig(key: String = "application") extends Configuration(ConfigurationContext(key)) with MetricsConfig {
      val applicationName = "Alerter"
      val alerts = {
        config.getSection("mojolly.borg.cadence.alerts") map { section =>
          section.keys.map(_.split("\\.", 2)).map(_.head) map { k =>
            Alert(
              target = section.getString(k + ".graphitekeypattern").get,
              warn = section.getInt(k + ".error").get,
              error = section.getInt(k + ".warn").get
            )
          }
        }
      } get
      val url = reporting map (r => "http://%s" format r.host) getOrElse "http://graphite"
      val period = config.getSection("mojolly.reporting") flatMap { sect ⇒
        sect.getInt("pollInterval")
      } getOrElse 60
    }

    case class Alert(target: String,  warn: Int, error: Int)

    case class GraphiteResponse(metrics: List[Metric])
    case class Metric(target: String, datapoints: List[Datapoint])
    case class Datapoint(timestamp: Long, value: Double)

    object GraphiteResponse {
      def apply(jsonString: String): Option[GraphiteResponse] = {
        parse(jsonString) match {
          case JArray(metric) => Some(GraphiteResponse(metric collect {
            case JObject(JField("target", JString(target)) :: JField("datapoints", JArray(datapoints)) :: Nil) =>
              Metric(target, datapoints collect {
                case JArray(List(JDouble(v), JInt(timestamp))) => Datapoint(timestamp.toLong, v)
              })
            }))
          case _ => None
        }
      }
    }
}