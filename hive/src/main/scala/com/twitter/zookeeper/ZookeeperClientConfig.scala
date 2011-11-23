package com.twitter.zookeeper

trait ZooKeeperClientConfig {
  def hostList: String
  val sessionTimeout: Int = 3000
  val basePath: String = ""
}