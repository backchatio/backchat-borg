package backchat.borg

trait LoadBalancer {

  protected def nodes: Iterable[Node]
  def nodeFor(service: String): Option[Node]
}

object LoadBalancer {
  val FirstRegistered = (nodes: Iterable[Node]) ⇒ new FirstRegistered(nodes)
  val LeastBusy = (nodes: Iterable[Node]) ⇒ new LeastBusy(nodes)
}
class FirstRegistered(protected val nodes: Iterable[Node]) extends LoadBalancer {
  def nodeFor(service: String) = {
    nodes find (_.services.contains(service))
  }
}

class LeastBusy(protected val nodes: Iterable[Node]) extends LoadBalancer {
  def nodeFor(service: String) = {
    (nodes
      filter (_.services contains service) headOption)
  }
}