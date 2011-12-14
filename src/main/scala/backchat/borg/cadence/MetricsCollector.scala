package backchat
package borg
package cadence

import net.liftweb.json._
import JsonDSL._
import org.joda.time.format.{ PeriodFormat, PeriodFormatter }
import org.joda.time.{ PeriodType }
import akka.actor._
import mojolly.metrics.UsesMetrics
import management.ManagementFactory
import java.lang.reflect.Method
import mojolly.ScheduledTask
import java.util.concurrent.TimeUnit
import org.hyperic.sigar.Sigar
import akka.routing.Listeners

object JmxProcessMetricsProvider {
  private val osMxBean = ManagementFactory.getOperatingSystemMXBean
  private val getMaxFileDescriptorCountField = {
    var method: Method = null
    try {
      method = osMxBean.getClass.getDeclaredMethod("getMaxFileDescriptorCount")
      method.setAccessible(true)
    } catch {
      case e: Exception ⇒ {
      }
    }
    method
  }

  private val getOpenFileDescriptorCountField = {
    var method: Method = null
    try {
      method = osMxBean.getClass.getDeclaredMethod("getOpenFileDescriptorCount")
      method.setAccessible(true)
    } catch {
      case e: Exception ⇒ {
      }
    }
    method
  }

  def maxFileDescriptorCount = {
    if (getMaxFileDescriptorCountField == null) -1L
    else {
      try {
        getMaxFileDescriptorCountField.invoke(osMxBean).asInstanceOf[Long]
      } catch {
        case _ ⇒ -1L
      }
    }
  }

  def openFileDescriptorCount = {
    if (getOpenFileDescriptorCountField == null) -1L
    else {
      try {
        getOpenFileDescriptorCountField.invoke(osMxBean).asInstanceOf[Long]
      } catch {
        case _ ⇒ -1L
      }
    }
  }
}

sealed trait Stat {
  def toJValue: JValue
}
case object StartMetrics
case object StopMetrics

object MetricsCollector extends Logging {

  lazy val sigar = {
    var s: Sigar = null
    try {
      s = new Sigar
      s.getPid
    } catch {
      case e ⇒ {
        logger.warn("Failed to load sigar", e)
        try {
          Option(s).foreach(_.close())
        } catch {
          case _ ⇒
        } finally {
          s = null
        }
      }
    }
    s
  }
}
abstract class MetricsCollector[StatsClass <: Stat](interval: Duration = 30.seconds)
    extends Actor with UsesMetrics with Logging with Listeners {
  private var _lastMeasured: Option[StatsClass] = None
  private var probeTasks: Option[ScheduledTask] = None

  import ProcessMetricsCollector.ProcessInfo

  override def preStart() {
    logger info "Started %s".format(getClass.getSimpleName)
    self ! StartMetrics
  }

  protected var processInfo: ProcessInfo = _

  protected def readLong(fn: StatsClass ⇒ Long, defaultValue: Long = -1L) = {
    _lastMeasured map fn getOrElse defaultValue
  }
  protected def readShort(fn: StatsClass ⇒ Short, defaultValue: Short = -1) = {
    _lastMeasured map fn getOrElse defaultValue
  }
  protected def sigar = MetricsCollector.sigar
  protected def startMetricsCollection()

  protected def stopMetricsCollection()

  protected def collectMetrics(): StatsClass

  protected def receive = {
    case StartMetrics ⇒ {
      logger trace "Starting to collect metrics in %s".format(getClass.getSimpleName)
      processInfo = ProcessInfo(sigar.getPid, JmxProcessMetricsProvider.maxFileDescriptorCount)
      probeTasks.foreach(_.stop())
      probeTasks =
        Some(
          ScheduledTask(
            Scheduler.schedule(self, 'probe, 10L, interval.getMillis, TimeUnit.MILLISECONDS)))
      startMetricsCollection()
    }
    case StopMetrics ⇒ {
      logger trace "Stopping to collect metrics in %s".format(getClass.getSimpleName)
      stopMetricsCollection()
      probeTasks foreach { _.stop() }
    }
    case 'probe ⇒ {
      logger trace "Probing for metrics in %s".format(getClass.getSimpleName)
      _lastMeasured = Some(collectMetrics())
      gossip(_lastMeasured.get)
    }
  }

}
object ProcessMetricsCollector {

  private val periodFormatter: PeriodFormatter = PeriodFormat.getDefault.withParseType(PeriodType.standard)
  private implicit def longWithPeriods(s: Long) = new {
    val period = new Period(s)
    def formatted = periodFormatter.print(period)
  }

  case class CpuInfo(percent: Short, sys: Long, user: Long, total: Long) extends Stat {
    def toJValue = {
      ("sys" -> sys.formatted) ~
        ("sysInMillis" -> sys) ~
        ("user" -> user.formatted) ~
        ("userInMillis" -> user) ~
        ("total" -> total.formatted) ~
        ("totatlInMillis" -> total)
    }
  }
  case class MemInfo(totalVirtual: Long, resident: Long, share: Long) extends Stat {
    def toJValue: JValue = {
      ("resident" -> resident) ~
        ("share" -> share) ~
        ("totalVirtual" -> totalVirtual)
    }
  }
  case class ProcessStats(timestamp: Long, openFileDescriptors: Long, cpu: CpuInfo, mem: MemInfo) extends Stat {
    def toJValue: JValue = {
      ("timestamp" -> timestamp) ~
        ("openFileDescriptors" -> openFileDescriptors) ~
        ("cpu" -> cpu.toJValue) ~
        ("mem" -> mem.toJValue)
    }
  }
  case class ProcessInfo(id: Long, maxFileDescriptors: Long) extends Stat {
    def toJValue: JValue = Extraction.decompose(this)
  }

}
class ProcessMetricsCollector(interval: Duration = 30.seconds)
    extends MetricsCollector[ProcessMetricsCollector.ProcessStats](interval) {
  import ProcessMetricsCollector._

  protected def startMetricsCollection() {
    metrics.gauge("app.openFileDescriptors") { readLong(_.openFileDescriptors) }
    metrics.gauge("app.memory.resident"){ readLong(_.mem.resident) }
    metrics.gauge("app.memory.share"){ readLong(_.mem.share) }
    metrics.gauge("app.memory.totalVirtual"){ readLong(_.mem.totalVirtual) }
    metrics.gauge("app.cpu.percent") { readShort(_.cpu.percent) }
    metrics.gauge("app.cpu.sys") { readLong(_.cpu.sys) }
    metrics.gauge("app.cpu.user") { readLong(_.cpu.user) }
    metrics.gauge("app.cpu.total") { readLong(_.cpu.total) }
  }

  protected def stopMetricsCollection() {
    List(
      "app.openFiledescriptors",
      "app.cpu.percent",
      "app.cpu.sys",
      "app.cpu.user",
      "app.cpu.total",
      "app.memory.resident",
      "app.memory.share",
      "app.memory.totalVirtual") foreach {
        metricsRegistry.removeMetric(metricsClass, _)
      }
  }

  protected def collectMetrics = {
    val cpui = sigar.getProcCpu(processInfo.id)
    val memi = sigar.getProcMem(processInfo.id)

    ProcessStats(
      System.currentTimeMillis(),
      JmxProcessMetricsProvider.openFileDescriptorCount,
      CpuInfo(cpui.getPercent.toShort, cpui.getSys, cpui.getUser, cpui.getTotal),
      MemInfo(memi.getSize, memi.getResident, memi.getShare))
  }

}

object SystemMetricsCollector {

  case class CpuStats(sys: Short, user: Short, idle: Short) extends Stat {
    def toJValue = Extraction.decompose(this)
  }
  case class MemStats(free: Long, freePct: Short, used: Long, usedPct: Short, actualFree: Long, actualUsed: Long) extends Stat {
    def toJValue = Extraction.decompose(this)
  }
  case class SwapStats(free: Long, used: Long) extends Stat {
    def toJValue = Extraction.decompose(this)
  }
  case class SystemStats(
      timestamp: Long, loadAverage: Seq[Double], uptime: Double, cpu: CpuStats, mem: MemStats, swap: SwapStats) extends Stat {
    def toJValue = Extraction.decompose(this)
  }
}

class SystemMetricsCollector(interval: Duration = 50.seconds)
    extends MetricsCollector[SystemMetricsCollector.SystemStats](interval) {

  import SystemMetricsCollector._

  protected def startMetricsCollection() {
    metrics.gauge("sys.cpu.sys") { readShort(_.cpu.sys) }
    metrics.gauge("sys.cpu.user") { readShort(_.cpu.user) }
    metrics.gauge("sys.cpu.idle") { readShort(_.cpu.idle) }
    metrics.gauge("sys.mem.free") { readLong(_.mem.free) }
    metrics.gauge("sys.mem.freePct") { readShort(_.mem.freePct) }
    metrics.gauge("sys.mem.used") { readLong(_.mem.used) }
    metrics.gauge("sys.mem.usedPct") { readShort(_.mem.usedPct) }
    metrics.gauge("sys.mem.actualFree") { readLong(_.mem.actualFree) }
    metrics.gauge("sys.mem.actualUsed") { readLong(_.mem.actualUsed) }
    metrics.gauge("sys.swap.free") { readLong(_.swap.free) }
    metrics.gauge("sys.swap.used") { readLong(_.swap.used) }
  }

  protected def stopMetricsCollection() = {
    List("cpu.sys", "cpu.user", "cpu.idle", "mem.free", "mem.freePct", "mem.used", "mem.usedPct",
      "mem.actualFree", "mem.actualUsed", "swap.free", "swap.used") foreach { met ⇒
        metricsRegistry.removeMetric(metricsClass, "sys." + met)
      }
  }

  protected def collectMetrics() = {
    val cpuPerc = sigar.getCpuPerc
    val mem = sigar.getMem
    val swap = sigar.getSwap
    def rp(v: ⇒ Double) = (v * 100).toShort
    SystemStats(
      System.currentTimeMillis(),
      sigar.getLoadAverage,
      sigar.getUptime.getUptime,
      CpuStats(rp(cpuPerc.getSys), rp(cpuPerc.getUser), rp(cpuPerc.getIdle)),
      MemStats(mem.getFree, mem.getFreePercent.toShort, mem.getUsed, mem.getUsedPercent.toShort, mem.getActualFree, mem.getActualUsed),
      SwapStats(swap.getFree, swap.getUsed))
  }
}