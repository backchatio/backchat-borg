package backchat
package borg
package zookeeper

import mojolly.Configuration

trait ClusterConfig { self: Configuration ⇒

  case class ClusterConfig(name: String)

}