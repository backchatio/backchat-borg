package com.twitter.zookeeper

import mojolly.LibraryImports._

trait ZookeeperClientConfig {
  def hostList: String
  val sessionTimeout: Duration = 3.seconds
  val basePath: String = ""
}