package backchat.borg
package hive

import akka.actor.Actor._

class InMemoryClusterClient(val serviceName: String, override val clientName: Option[String] = None) extends ClusterClient with ClusterNotificationManagerComponent
    with InMemoryClusterManagerComponent {
  val clusterNotificationManager = actorOf(new ClusterNotificationManager)
  val clusterManager = actorOf(new InMemoryClusterManager)
}