package com.twitter.zookeeper

import akka.actor.ActorRef

trait ZooKeeperClientConfig {
  def hostList: String
  val sessionTimeout: Int = 3000
  val basePath: String = ""
  val rootNode: String = ""
  private val emptyListeners = Vector.empty[ActorRef]
  def listeners = emptyListeners
}