package backchat.borg.cadence

import com.weiglewilczek.slf4s.Logger
import com.ning.http.client.AsyncHttpClient
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}
import net.liftweb.json._
import mojolly._
import metrics.MetricsConfig

/**
 * Application for alerting.
 */
object Alerter extends App {
  lazy val loggerName = "ALERT"
  lazy val logger = Logger(loggerName)

  val poller = new GraphitePoller(new AlerterConfig) start()
  sys.addShutdownHook { poller.shutdown }

  class GraphitePoller(config: AlerterConfig) extends Runnable {
    lazy val executor: ScheduledExecutorService = Executors newSingleThreadScheduledExecutor()
    lazy val httpClient = new AsyncHttpClient

    def start() = {
      executor scheduleAtFixedRate(this, 0, config.period, TimeUnit.SECONDS)
      this
    }

    def shutdown() = {
      executor shutdown()
      httpClient.close()
    }

    def requestUrl = "%s/render?format=json&from=-%ds" format (config.url, config.period)

    def run {
      val req = (httpClient prepareGet requestUrl)
      config.alerts.get foreach { a =>
        req addQueryParameter("target", a.target)
      }
      val resp = req execute() get()
      val json = resp.getResponseBody
      val gr = GraphiteResponse(json)
    }
  }
  
  class AlerterConfig(key: String = "application") extends Configuration(ConfigurationContext(key)) with MetricsConfig {
    val applicationName = "Alerter"
    val alerts = {
      config.getSection("mojolly.borg.cadence.alerts") map { section =>
        section.keys.map(_.split("\\.")).map(_.head) map { k =>
          Alert(
            target = section.getString(k + ".graphitekeypattern").get,
            warn = section.getInt(k + ".error").get,
            error = section.getInt(k + ".warn").get
          )
        }
      }
    }
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
              case JArray(List(x: JValue, JInt(timestamp))) => Datapoint(timestamp.toLong, x match {
                case JDouble(v) => v
                case _ => 0.0
              })
            })
          }))
        case _ => None
      }
    }
  }
}