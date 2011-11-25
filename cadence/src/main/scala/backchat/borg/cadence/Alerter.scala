package backchat.borg.cadence

import com.weiglewilczek.slf4s.Logger
import com.ning.http.client.AsyncHttpClient
import java.lang.Thread
import java.util.concurrent.{TimeUnit, Executors, ScheduledExecutorService}
import scopt.OptionParser
import net.liftweb.json._

/**
 * Application for alerting.
 */
object Alerter extends App {
  lazy val loggerName = "ALERT"
  lazy val logger = Logger(loggerName)

  lazy val config = new Config

  lazy val parser = new OptionParser("scopt") {
    intOpt("p", "period", "polling period in seconds", {v: Int => config.period = v})
    opt("u", "url", "Graphite URL", {v: String => config.url = v})
  }
  
  parser parse args
  val poller = new GraphitePoller(config.url, logger, config.period, TimeUnit.SECONDS) start()
  addShutdownHook()

  def addShutdownHook() = {
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      def run = {
        poller.shutdown
      }
    }))
  }

  class GraphitePoller(url: String, logger: Logger, pollingPeriod: Long, pollingTimeUnit: TimeUnit) extends Runnable {
    lazy val executor: ScheduledExecutorService = Executors newSingleThreadScheduledExecutor()
    lazy val httpClient = new AsyncHttpClient

    def start() = {
      executor scheduleAtFixedRate(this, 0, pollingPeriod, pollingTimeUnit)
      this
    }

    def shutdown() = {
      executor shutdown()
      httpClient.close()
    }

    def requestUrl = "%s/render?format=json" format url

    def run {
      val resp = (httpClient prepareGet requestUrl) execute() get()
      val gr = GraphiteResponse(resp.getResponseBody)
      println(gr)
    }
  }
  
  class Config(var url: String = "http://graphite", var period: Int = 60)
  
  case class GraphiteResponse(metrics: List[Metric])
  case class Metric(target: String, datapoints: List[Datapoint])
  case class Datapoint(timestamp: Long, value: Double)

  object GraphiteResponse {
    def apply(jsonString: String): Option[GraphiteResponse] = {
      parse(jsonString) match {
        case JArray(metric) => Some(GraphiteResponse(metric collect {
          case JObject(JField("target", JString(target)) :: JField("datapoints", JArray(datapoints)) :: Nil) =>
            val dp = datapoints collect {
              case JArray(List(JDouble(value), JInt(timestamp))) => Datapoint(timestamp.toLong, value)
            }
            Metric(target, dp)
          }))
        case _ => None
      }
    }
  }
}