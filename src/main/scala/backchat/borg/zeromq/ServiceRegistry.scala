package backchat
package borg
package zeromq

import akka.actor._
import Messages._
import collection.immutable.Queue

case class ServiceListRequest(m: ProtocolMessage)
case class ServiceList(services: List[String], m: ProtocolMessage)
class ServiceRegistry(initialRegistry: Map[String, Queue[String]] = Map.empty, callback: Option[ActorRef] = None) extends Actor with Logging {

  protected var registry = initialRegistry

  protected def manageRegistry: Receive = {
    case m@RegisterService(name, id) ⇒ {
      logger debug "Registering worker '%s' to service '%s'.".format(id, name)
      if (registry.contains(name)) {
        val queue = registry(name)
        if (!queue.contains(id)) registry += name -> (queue enqueue id)
      } else {
        registry += name -> Queue(id)
      }
      notifyCallback('registered -> registry(name))
      logger info "Registered worker '%s' to service '%s'.".format(id, name)
    }
    case m@UnregisterService(name, id) ⇒ {
      logger debug "Unregistering worker '%s' from service '%s'.".format(id, name)
      if (registry.contains(name)) {
        val queue = registry(name)
        if (queue.size > 1 && queue.contains(id)) registry += name -> queue.filterNot(_ == id)
        else if (queue.size == 1 && queue.contains(id)) registry -= name
        logger info "Unregistered worker '%s' from service '%s'.".format(id, name)
      } else {
        logger debug "Service '%s' wasn't registered yet.".format(name)
      }
      notifyCallback('unregistered -> registry.get(name))
    }
    case Enqueue(name, event, _) ⇒ {
      forwardToService(name, event)
    }
    case request: Request ⇒ {
      forwardToService(request.target, request.event)
    }
    case ServiceListRequest(m) ⇒ {
      self.sender foreach { _ ! ServiceList(registry.keys.toList, m) }
    }
  }

  protected def receive = manageRegistry

  protected def notifyCallback(msg: Any) {
    callback foreach { _ ! msg }
  }

  protected def forwardToService[MsgType](name: String, message: MsgType) {
    logger debug "forwarding to service '%s': %s".format(name, message)
    registry.get(name) foreach { ids ⇒
      logger debug "got service '%s': %s".format(name, ids.mkString("[", ",", "]"))
      if (ids.size > 0) {
        val (id, dequeued) = ids.dequeue
        logger debug "The sender: %s".format(self.sender)
        Actor.registry.actorsFor(id).headOption foreach { _ forward message }
        if (ids.size > 1) registry += name -> (dequeued enqueue id) // move the id to the back of the set
        notifyCallback('forwarded -> registry(name))
      }
    }
  }
}
