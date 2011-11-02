package backchat.borg
package cluster

import akka.actor._

trait InMemoryClusterManagerComponent extends ClusterManagerComponent with ClusterManagerHelper {
  this: ClusterNotificationManagerComponent ⇒

  class InMemoryClusterManager extends Actor {
    import ClusterManagerMessages._
    private var currentNodes = scala.collection.mutable.Map[Int, Node]()
    private var available = scala.collection.mutable.Set[Int]()

    override def preStart() {
      clusterNotificationManager ! ClusterNotificationMessages.Connected(currentNodes)
    }

    protected def receive = {
      case AddNode(node) ⇒ if (currentNodes.contains(node.id)) {
        self reply ClusterManagerResponse(Some(new InvalidNodeException("A node with id %d already exists".format(node.id))))
      } else {
        val n = if (available.contains(node.id)) node.copy(available = true) else node.copy(available = false)

        currentNodes += (n.id -> n)
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        self reply ClusterManagerResponse(None)
      }

      case RemoveNode(nodeId) ⇒
        currentNodes -= nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        self reply ClusterManagerResponse(None)

      case MarkNodeAvailable(nodeId) ⇒
        currentNodes.get(nodeId).foreach { node ⇒ currentNodes.update(nodeId, node.copy(available = true)) }
        available += nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        self reply ClusterManagerResponse(None)

      case MarkNodeUnavailable(nodeId) ⇒
        currentNodes.get(nodeId).foreach { node ⇒ currentNodes.update(nodeId, node.copy(available = false)) }
        available -= nodeId
        clusterNotificationManager ! ClusterNotificationMessages.NodesChanged(currentNodes)
        self reply ClusterManagerResponse(None)

      case Shutdown ⇒ self.stop()

    }

  }
}