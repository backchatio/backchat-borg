package backchat.borg
package cluster

import akka.actor.Actor._

class ZooKeeperClusterClient(override val clientName: Option[String], val serviceName: String, zooKeeperConnectString: String, zooKeeperSessionTimeoutMillis: Duration) extends ClusterClient
    with ClusterNotificationManagerComponent with ZooKeeperClusterManagerComponent {
  def clusterNotificationManager = actorOf(new ClusterNotificationManager())

  def clusterManager = actorOf(new ZooKeeperClusterManager(zooKeeperConnectString, zooKeeperSessionTimeoutMillis, serviceName))
}