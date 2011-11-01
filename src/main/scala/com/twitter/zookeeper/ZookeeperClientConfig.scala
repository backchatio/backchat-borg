package com.twitter.zookeeper

trait ZookeeperClientConfig {
  def hostList: String
  val sessionTimeout: Int = 3000
  val basePath: String = ""
}