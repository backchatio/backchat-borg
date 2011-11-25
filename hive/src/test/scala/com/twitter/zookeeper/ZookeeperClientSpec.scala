package com.twitter.zookeeper

import mojolly.LibraryImports._
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.CreateMode._
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.data.Id
import scala.collection.mutable
import backchat.borg.hive.{ZooKeeperClientContext, ZooKeeperSpecification}
import mojolly.testing.TimeHelpers


class ZooKeeperClientSpec extends ZooKeeperSpecification {
  
  def is = 
    "A ZooKeeperClient should" ^
      "be able to be instantiated with a FakeWatcher" ! specify.instantiatesWithFake ^
      "connect to a local ZooKeeper server and retrieve the version" ! specify.retrievesVersion ^
      "get data at a known-good specified path" ! specify.getsDataFromKnownGood ^
      "get data at a known-bad specified path" ! specify.throwsForGetDataFromKnownBad ^
      "get a list of children" ! specify.getsChildren ^
      "create a node at a specified path"  ! specify.createsNodeAtPath ^
      "watch a node" ! specify.watchesNode ^
      "watch a tree of nodes" ! specify.watchesTreeOfNodes ^
      "watch a tree of nodes with data" ! specify.watchesTreeOfNodesWithData ^
      end


  def specify = ZooKeeperSpecContext(zookeeperServer.port)

  case class ZooKeeperSpecContext(port: Int) extends ZooKeeperClientContext(port) with TimeHelpers {

    def instantiatesWithFake = this { zkClient must not beNull }
    
    def retrievesVersion = this { zkClient.isAlive must beTrue }
    
    def getsDataFromKnownGood = this {
      val results: Array[Byte] = zkClient.get("/")
      results.size must beGreaterThanOrEqualTo(0)
    }
    
    def throwsForGetDataFromKnownBad = this {
      zkClient.get("/thisdoesnotexist") must throwA[NoNodeException]
    }
    
    def getsChildren = this {
      zkClient.getChildren("/") must not beEmpty
    }
    
    def createsNodeAtPath = this {
      val data: Array[Byte] = Array(0x63)
      val id = new Id("world", "anyone")
      val createMode = EPHEMERAL

      val res = zkClient.create("/foo", data, createMode) mustEqual "/foo"
      zkClient.delete("/foo")
      res
    }
    
    def watchesNode = this {
      val data: Array[Byte] = Array(0x63)
      val node = "/datanode"
      val createMode = EPHEMERAL
      var watchCount = 0
      def watcher(data : Option[Array[Byte]]) {
        watchCount += 1
      }
      zkClient.create(node, data, createMode)
      zkClient.watchNode(node, watcher)
      sleep -> 50.millis
      val res = watchCount mustEqual 1
      zkClient.delete("/datanode")
      res
    }
    
    def watchesTreeOfNodes = this {
      var children : Seq[String] = List()
      var watchCount = 0
      def watcher(nodes : Seq[String]) {
        watchCount += 1
        children = nodes
      }
      zkClient.createPath("/tree/a")
      zkClient.createPath("/tree/b")
      zkClient.watchChildren("/tree", watcher)
      val r1 = children.size must_== 2
      val r2 = children must haveTheSameElementsAs(List("a", "b"))
      val r3 = watchCount must_== 1
      zkClient.createPath("/tree/c")
      sleep -> 50.millis
      val r4 = children.size must_== 3
      val r5 = children must haveTheSameElementsAs(List("a", "b", "c"))
      val r6 = watchCount must_== 2
      zkClient.delete("/tree/a")
      sleep -> 50.millis
      val r7 = children.size must_== 2
      val r8 = children must haveTheSameElementsAs(List("b", "c"))
      val r9 = watchCount must_== 3
      zkClient.deleteRecursive("/tree")
      r1 and r2 and r3 and r4 and r5 and r6 and r7 and r8 and r9
    }
    
    def watchesTreeOfNodesWithData = this {
      def mkNode(node : String) {
        zkClient.create("/root/" + node, node.getBytes, CreateMode.EPHEMERAL)
      }
      var children : mutable.Map[String,String] = mutable.Map()
      var watchCount = 0
      def notifier(child : String) {
        watchCount += 1
        if (children.contains(child)) {
          children(child) mustEqual child
        }
      }
      zkClient.createPath("/root")
      mkNode("a")
      mkNode("b")
      zkClient.watchChildrenWithData("/root", children,
                                     {(b : Array[Byte]) => new String(b)}, notifier)
      val r1 = children.size must_== 2
      val r2 = children.keySet must haveTheSameElementsAs(List("a", "b"))
      val r3 = watchCount must_== 2
      mkNode("c")
      sleep -> 50.millis
      val r4 = children.size must_== 3
      val r5 = children.keySet must haveTheSameElementsAs(List("a", "b", "c"))
      val r6 = watchCount must_== 3
      zkClient.delete("/root/a")
      sleep -> 50.millis
      val r7 = children.size must_== 2
      val r8 = children.keySet must haveTheSameElementsAs(List("b", "c"))
      val r9 = watchCount must_== 4
      zkClient.deleteRecursive("/root")
      r1 and r2 and r3 and r4 and r5 and r6 and r7 and r8 and r9
    }
    
    
  }
}