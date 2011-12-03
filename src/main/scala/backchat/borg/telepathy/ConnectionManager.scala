package backchat.borg
package telepathy

import scalaz._
import Scalaz._
import mojolly.metrics.UsesMetrics
import collection.SortedSet

object ConnectionManager {

  object AvailableServers {

    def apply(servers: (String, AvailableServer)*) = new AvailableServers(servers: _*)
    def empty = new AvailableServers
  }
  class AvailableServers(servers: Map[String, AvailableServer]) extends Map[String, AvailableServer] {
    def this(srvrs: (String, AvailableServer)*) = this(Map(srvrs: _*))
    private val ss = servers

    def iterator = ss.iterator

    def +[B1 >: AvailableServer](kv: (String, B1)) = new AvailableServers(ss + kv.asInstanceOf[(String, AvailableServer)])

    def -(key: String) = new AvailableServers(ss - key)

    def get(key: String) = ss.get(key)

  }
  case class AvailableServer(endpoint: String, ttl: Duration = 2.seconds)

  type RequestRouter = Set[ActiveServer] ⇒ Option[ActiveServer]

  object LRURequestRouter extends RequestRouter {

    implicit def ordering = Ordering.fromLessThan[ActiveServer] { (left, right) ⇒
      if (left.nextPing == right.nextPing) left.responseTime < right.responseTime
      else left.nextPing < right.nextPing
    }

    def apply(activeServers: Set[ActiveServer]) = SortedSet(activeServers.toSeq: _*)(ordering).headOption
  }

  object ActiveServers {
    def empty = new ActiveServers
  }
  class ActiveServers(servers: Set[ActiveServer], route: RequestRouter = LRURequestRouter) extends Set[ActiveServer] {
    def this(srvrs: ActiveServer*) = this(Set(srvrs: _*))
    private val ss = servers
    def iterator = ss.iterator

    def -(elem: ActiveServer) = new ActiveServers(ss.filterNot(_.server == elem.server).toSeq: _*)

    def +(elem: ActiveServer) = {
      val sss = ss.filterNot(_.server.endpoint == elem.server.endpoint)
      new ActiveServers(sss + elem)
    }
    def pickServer = route(ss)
    def withoutExpired = new ActiveServers(ss.filter(_.expires >= DateTime.now))
    def thatNeedAPing = ss.filter(_.nextPing <= DateTime.now)
    val smallestPingTimeout = {
      val timeouts = SortedSet(ss.toSeq: _*)(Ordering.fromLessThan(_.server.ttl.millis < _.server.ttl.millis))
      timeouts.headOption map (_.server.ttl.millis) getOrElse 2000L
    }

    def contains(elem: ActiveServer) = ss.contains(elem)
  }
  case class ActiveServer(server: AvailableServer, endpoint: String, responseTime: Duration, nextPing: DateTime = MIN_DATE, expires: DateTime = MIN_DATE)

  case class ActiveRequest(req: BorgMessage, server: String, created: DateTime, expires: DateTime, retries: Int = 0)

  implicit val ConnectionManagerResource = resource((cm: ConnectionManager) ⇒ cm.close)

  def apply(initialServers: AvailableServers = AvailableServers.empty) = new DefaultConnectionManager(initialServers)

  private[telepathy] class DefaultConnectionManager(initialServers: AvailableServers = AvailableServers.empty) extends ConnectionManager {
    private var _availableServers = initialServers
    private var _activeServers = ActiveServers.empty

    def availableServers = _availableServers

    def activeServers = _activeServers

    def addAvailableServer(name: String, address: String, ttl: Duration) = {
      val avail = AvailableServer(address, ttl)
      _availableServers += name -> avail
      avail
    }

    def removeAvailableServer(name: String) {
      _availableServers -= name
    }

    def close = {
      _availableServers = AvailableServers.empty
      _activeServers = ActiveServers.empty
    }
  }

  trait ConnectionManager extends Logging with UsesMetrics {

    def availableServers: AvailableServers
    def activeServers: ActiveServers

    def addAvailableServer(name: String, address: String, ttl: Duration = 2.seconds): AvailableServer
    def removeAvailableServer(address: String)

    def close

    def isAlive = availableServers.nonEmpty
  }
}

