/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package backchat.borg
package hive

import akka.actor.ActorRef

trait ClusterManagerComponent {
  def clusterManager: ActorRef

  sealed trait ClusterManagerMessage
  object ClusterManagerMessages {
    case class AddNode(node: Node) extends ClusterManagerMessage
    case class RemoveNode(nodeId: Int) extends ClusterManagerMessage
    case class MarkNodeAvailable(nodeId: Int) extends ClusterManagerMessage
    case class MarkNodeUnavailable(nodeId: Int) extends ClusterManagerMessage
    case class AddService(name: String, nodeId: Int) extends ClusterManagerMessage
    case class RemoveService(name: String, nodeId: Int) extends ClusterManagerMessage
    case class MarkServiceActive(name: String, nodeId: Int) extends ClusterManagerMessage
    case class MarkServiceInactive(name: String, nodeId: Int) extends ClusterManagerMessage
    
//    abstract class HealthMetric(val name: String, val value: Double) extends ClusterManagerMessage
//    case class CpuUsage(value: Double) extends HealthMetric("cpu-usage", value)
//    case class CpuIdle(value: Double) extends HealthMetric("cpu-idle", value)
//    case class MemUsage(value: Double) extends HealthMetric("mem-usage", value)
//    case class MemFree(value: Double) extends HealthMetric("mem-free", value)
   
    case object Shutdown extends ClusterManagerMessage

    case class ClusterManagerResponse(exception: Option[ClusterException]) extends ClusterManagerMessage
  }

}