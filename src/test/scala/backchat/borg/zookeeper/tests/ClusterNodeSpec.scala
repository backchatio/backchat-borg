package backchat
package borg
package zookeeper
package tests

import org.specs2._

class ClusterNodeSpec extends Specification {

  def is=
    "A ClusterNode should" ^
      "have a name and configuration properties" ! pending ^
      "register with the zookeeper service" ! pending ^
      "unregister from the zookeeper service" ! pending ^ end
}