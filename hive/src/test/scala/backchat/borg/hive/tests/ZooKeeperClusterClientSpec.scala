//package backchat
//package borg
//package hive
//package tests
//
//import org.specs2.Specification
//import org.apache.zookeeper.data.Stat
//import org.specs2.mock.Mockito
//import akka.actor._
//import Actor._
//import org.specs2.execute.Result
//import org.specs2.specification.{Step, Fragments, Around}
//import testing.ZooKeeperTestServer
//import org.apache.zookeeper.{ZooDefs, CreateMode, ZooKeeper, Watcher}
//import collection.mutable.ListBuffer
//import java.util.ArrayList
//
//
//class ZooKeeperClusterClientSpec extends Specification { def is =
//
//  "A ZooKeeperClusterClient should" ^
//    "instantiate a ZooKeeper instance when started" ! context.instantiatesOnStart ^
//    "when a Connected message is received" ^
//      "verify the ZooKeeper structure by" ^
//        "doing nothing if all znodes exist" ! context.doNothingOnConnect ^
//        "creating the cluster, membership and availabillity znodes if they do not yet exist" ! context.createsNodesIfMissing ^ bt ^
//      "calculate the current nodes" ! context.calculatesCurrentNodes ^
//      "send a notification to the notification manager actor" ! context.sendsNotificationForConnected ^ bt ^
//    "when a Disconnected message is received" ^
//      "send a notification to the notification manager actor" ! context.sendsNotificationForDisconnected ^
//      "do nothing if not connected" ! context.doNothingIfAlreadyDisconnected ^ bt ^
//    "when an Expired message is received" ^
//      "reconnect to ZooKeeper" ! context.reconnectsOnExpired ^ bt ^
//    "when a NodeChildrenChanged message is received" ^
//      "and the availability node changed" ^
//        "update the node availability and notify listeners" ! context.updatesAvailabilityAndNotifiesListeners ^
//        "handle the case that all nodes are unavailable" ! context.handlesAllNodesUnavailable ^
//        "do nothing if not connected" ! context.doNothingWithAvailabilityChangeWhenDisconnected ^ bt ^
//      "update the nodes and notify listeners" ! context.updatesNodesAndNotifies ^
//      "handle the case that a node is removed" ! context.handlesNodeIsRemoved ^
//      "handle the case that a node is removed" ! context.handlesNodeIsRemoved2 ^
//      "do nothing if not connected" ! context.doNothingWithMembershipChangeWhenDisconnected ^ bt ^
//    "when a Shutdown message is received" ^
//      "stop handling events" ! context.stopHandlingEventsOnShutdown ^ bt ^
//    "when a AddNode message is received" ^
//      "throw a ClusterDisconnectedException if not connected" ! context.throwsClusterDisconnectedForAddNode ^
//      "throw an InvalidNodeException if the node already exists" ! context.throwsInvalidNodeForAddNode ^
//      "add the node to zookeeper" ! context.addsNode ^
//      "notify listeners of list change" ! context.notifiesForAddNode ^
//    "when a RemoveNode message is received" ^
//      "throw a ClusterDisconnectedException if not connected" ! context.throwsClusterDisconnectedForRemoveNode ^
//      "do nothing if the node does not exist in ZooKeeper" ! context.doNothingIfNodeDoesNotExist ^
//      "remove the node from ZooKeeper if the node exists" ! context.removesNode ^
//      "notify listeners of list change" ! context.notifiesForRemoveNode ^
//    "when a MarkAvailable message is received" ^
//      "throw a ClusterDisconnectedException if not connected" ! context.throwsClusterDisconnectedForMarkAvailable ^
//      "add the node to zookeeper if it doesn't exist" ! context.addsNodeForAvailable ^
//      "do nothing if the znode already exists" ! context.doNothingIfAlreadyExists ^
//      "notify listeners of list change" ! context.notifiesForAvailable ^
//    "when a MarkAvailable message is received" ^
//      "throw a ClusterDisconnectedException if not connected" ! context.throwsClusterDisconnectedForMarkUnavailable ^
//      "remove the node from zookeeper if it exists" ! context.removesNodeForUnavailable ^
//      "do nothing if the znode doesn't exists" ! context.doNothingIfDoesNotExist ^
//      "notify listeners of list change" ! context.notifiesForUnavailable ^
//  end
//
//
////  val zooKeeperServer = new ZooKeeperTestServer()
////  override def map(fs: => Fragments) = Step(zooKeeperServer.start()) ^ fs ^ Step(zooKeeperServer.stop())
//  override def map(fs: => Fragments) = super.map(fs ^ Step(registry.shutdownAll()))
//
//  def context = new ZooKeeperClusterClientSpecContext
//
//  class ZooKeeperClusterClientSpecContext extends Mockito with Around
//      with ZooKeeperClusterManagerComponent with ClusterNotificationManagerComponent{
//    import ZooKeeperMessages._
//    import ClusterManagerMessages._
//
//    val mockZooKeeper = smartMock[ZooKeeper]
//
//    var connectedCount = 0
//    var disconnectedCount = 0
//    var nodesChangedCount = 0
//    var shutdownCount = 0
//    var nodesReceived: Set[Node] = Set()
//
//    def zkf(connectString: String, sessionTimeout: Duration, watcher: Watcher) = mockZooKeeper
//    val clusterManager = actorOf(new ZooKeeperClusterManager("", new Duration(0), "test")(zkf))
////    val clusterManager = actorOf(new ZooKeeperClusterManager("localhost:%s".format(zooKeeperServer.port), new Duration(3000), "test"))
//
//    val rootNode = "/test"
//    val membershipNode = rootNode + "/members"
//    val availabilityNode = rootNode + "/available"
//
//    val clusterNotificationManager = actorOf(new Actor {
//      protected def receive = {
//        case ClusterNotificationMessages.Connected(nodes) => connectedCount += 1; nodesReceived = nodes
//        case ClusterNotificationMessages.Disconnected => disconnectedCount += 1
//        case ClusterNotificationMessages.NodesChanged(nodes) => nodesChangedCount += 1; nodesReceived = nodes
//        case ClusterNotificationMessages.Shutdown => shutdownCount += 1; self.stop()
//      }
//    })
//
//    def around[T](t: => T)(implicit evidence$1: (T) => Result) = {
//      clusterManager.start()
//      clusterNotificationManager.start()
//      val res = t
//      clusterManager ! Shutdown
//      clusterNotificationManager ! Shutdown
//      res
//    }
//
//    def instantiatesOnStart = {
//      var callCount = 0
//      def countedZkf(connectString: String, sessionTimeout: Duration, watcher: Watcher) = {
//        callCount += 1
//        mockZooKeeper
//      }
//      val zkm = actorOf(new ZooKeeperClusterManager("", new Duration(0L), "")(countedZkf _))
//      zkm.start
//      val res = callCount must eventually(be_==(1))
//      zkm ! Shutdown
//      res
//    }
//
//    val znodes =  List(rootNode, membershipNode, availabilityNode)
//    def doNothingOnConnect =  {
////      znodes.foreach(mockZooKeeper.exists(_, false) returns mock[Stat])
////
////      clusterManager ! Connected
////      Thread.sleep(10)
////
////      znodes.map(there was one(mockZooKeeper).exists(_, false)).reduce(_ and _)
//      pending
//    }
//
//
//    def createsNodesIfMissing =  {
////      znodes.foreach { path =>
////        mockZooKeeper.exists(path, false) returns null
////        mockZooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
////      }
////
////      clusterManager ! Connected
////      Thread sleep 10
////
////      znodes map { path =>
////        there was one(mockZooKeeper).exists(path, false) and
////        (there was one(mockZooKeeper).create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT))
////      } reduce (_ and _)
//      pending
//    }
//
//    def calculatesCurrentNodes = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val availability = membership.clone.asInstanceOf[ArrayList[String]]
//      availability.remove(2)
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns availability
//
//      clusterManager ! Connected
//      Thread sleep 50
//
//      got {
//        (one(mockZooKeeper).getChildren(membershipNode, true))
//        (nodes map {node =>
//          one(mockZooKeeper).getData("%s/%d".format(membershipNode, node.id), false, null)
//        })
//        (one(mockZooKeeper).getChildren(availabilityNode, true))
//      }
//
//    }
//
//    def sendsNotificationForConnected = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val availability = membership.clone.asInstanceOf[ArrayList[String]]
//      availability.remove(1)
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns availability
//
//      clusterManager ! Connected
//      Thread sleep 50
//
//      (connectedCount must eventually(be_==(1))) and
//      (nodesReceived.size must be_==(3)) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map (node => node must be_==(nodes(node.id - 1))) reduce (_ and _))
//    }
//
//    def sendsNotificationForDisconnected = this {
//      clusterManager ! Connected
//      clusterManager ! Disconnected
//
//      disconnectedCount must eventually(be_==(1))
//    }
//
//    def doNothingIfAlreadyDisconnected = this {
//      clusterManager ! Disconnected
//
//      disconnectedCount must eventually(be_==(0))
//    }
//
//    def reconnectsOnExpired = {
//      var callCount = 0
//      def countedZkf(connectString: String, sessionTimeout: Duration, watcher: Watcher) = {
//        callCount += 1
//        mockZooKeeper
//      }
//
//      val zkm = actorOf(new ZooKeeperClusterManager("", new Duration(0), "")(countedZkf _))
//      zkm.start
//      zkm ! Connected
//      zkm ! Expired
//
//      val res = callCount must eventually(be_==(2))
//
//      zkm ! Shutdown
//
//      res
//    }
//
//    def handlesAllNodesUnavailable = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val newAvailability = new ArrayList[String]
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns membership thenReturns newAvailability
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_==(3))) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map { _.available must beTrue } reduce (_ and _))
//
//      clusterManager ! NodeChildrenChanged(availabilityNode)
//
//      val res2 = (nodesChangedCount must eventually(be_==(1)))
//      (nodesReceived.size must be_==(3)) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map { n => n.available must beFalse } reduce (_ and _))
//
//      res1 and res2 and (there were two(mockZooKeeper).getChildren(availabilityNode, true))
//
//    }
//
//    def updatesAvailabilityAndNotifiesListeners = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val availability = new ArrayList[String]
//      availability.add("2")
//
//      val newAvailability = new ArrayList[String]
//      newAvailability.add("1")
//      newAvailability.add("3")
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns availability thenReturns newAvailability
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_==(3))) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map { n =>
//        if (n.id == 2) n.available must beTrue else n.available must beFalse
//      } reduce (_ and _))
//
//      clusterManager ! NodeChildrenChanged(availabilityNode)
//
//      val res2 = (nodesChangedCount must eventually(be_==(1))) and
//      (nodesReceived.size must be_==(3)) and
//      (nodesReceived must haveTheSameElementsAs(nodes))
//      (nodesReceived map { n =>
//        if (n.id == 2) n.available must beFalse else n.available must beTrue
//      } reduce (_ and _))
//
//      res1 and res2 and (there were two(mockZooKeeper).getChildren(availabilityNode, true))
//
//    }
//
//    def doNothingWithAvailabilityChangeWhenDisconnected = this {
//      clusterManager ! NodeChildrenChanged(availabilityNode)
//      nodesChangedCount must eventually(be_==(0))
//    }
//
//    def doNothingWithMembershipChangeWhenDisconnected = this {
//      clusterManager ! NodeChildrenChanged(membershipNode)
//      nodesChangedCount must eventually(be_==(0))
//    }
//
//    def updatesNodesAndNotifies = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//
//      val newMembership = new ArrayList[String]
//      newMembership.add("1")
//      newMembership.add("2")
//      newMembership.add("3")
//
//      val updatedNodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
//      val nodes = updatedNodes.slice(0, 2)
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
//      updatedNodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns membership
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_==(2))) and (nodesReceived must haveTheSameElementsAs(nodes))
//
//      clusterManager ! NodeChildrenChanged(membershipNode)
//
//      val res2 = (nodesChangedCount must eventually(be_==(1))) and
//      (nodesReceived.size must be_==(3)) and
//      (nodesReceived must haveTheSameElementsAs(updatedNodes))
//
//      res1 and res2 and (got {
//        two(mockZooKeeper).getChildren(availabilityNode, true)
//        two(mockZooKeeper).getChildren(membershipNode, true)
//      })
//    }
//
//    def handlesNodeIsRemoved = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val newMembership = new ArrayList[String]
//      newMembership.add("1")
//      newMembership.add("3")
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns membership
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_==(3))) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map { _.available must beTrue } reduce (_ and _))
//
//      clusterManager ! NodeChildrenChanged(membershipNode)
//
//      val res2 = nodesChangedCount must eventually(be_==(1))
//      nodesReceived.size must be_==(2)
//      nodesReceived must haveTheSameElementsAs(List(nodes(0), nodes(2)))
//
//      res1 and res2 and (there were two(mockZooKeeper).getChildren(membershipNode, true))
//
//    }
//
//    def handlesNodeIsRemoved2 = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val newMembership = new ArrayList[String]
//      newMembership.add("1")
//      newMembership.add("3")
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//        Node(2, "localhost:31314", true, Set(2, 3)), Node(3, "localhost:31315", false, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership thenReturns newMembership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns membership
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_==(3))) and
//      (nodesReceived must haveTheSameElementsAs(nodes)) and
//      (nodesReceived map { _.available must beTrue } reduce (_ and _))
//
//      clusterManager ! NodeChildrenChanged(membershipNode)
//
//      val res2 = (nodesChangedCount must eventually(be_==(1))) and
//      (nodesReceived.size must be_==(2)) and
//      (nodesReceived must haveTheSameElementsAs(List(nodes(0), nodes(2))))
//
//      res1 and res2 and (there were two(mockZooKeeper).getChildren(membershipNode, true))
//    }
//
//    def stopHandlingEventsOnShutdown =  {
//      pending
//////      doNothing.when(mockZooKeeper).close
////      var callCount = 0
////      def countedZkf(connectString: String, sessionTimeout: Duration, watcher: Watcher) = {
////        callCount += 1
////        mockZooKeeper
////      }
////
////      val zkm = actorOf(new ZooKeeperClusterManager("", new Duration(0), "")(countedZkf _))
////      zkm.start
//////      clusterManager ! Shutdown
//////      clusterManager ! Connected
////
////      Thread sleep 10
////      val res = (callCount must eventually(be_==(1))) and (there was one(mockZooKeeper).close)
////
////      zkm ! Shutdown
////      res
//    }
//
//    val node = Node(1, "localhost:31313", false, Set(1, 2))
//
//    def throwsClusterDisconnectedForAddNode = this {
//      val ex = (clusterManager ? AddNode(node)).as[ClusterManagerResponse].get.exception
//
//      ex must beSome[ClusterException].which(_ must haveClass[ClusterDisconnectedException])
//    }
//
//    def throwsInvalidNodeForAddNode = this {
//      val path = membershipNode + "/1"
//      mockZooKeeper.exists(path, false) returns mock[Stat]
//
//      clusterManager ! Connected
//      val ex = (clusterManager ? AddNode(node)).as[ClusterManagerResponse].get.exception
//
//      (ex must beSome[ClusterException].which(_ must haveClass[InvalidNodeException])) and
//      (there was one(mockZooKeeper).exists(path, false))
//    }
//
//    def addsNode = this {
//      val path = membershipNode + "/1"
//      mockZooKeeper.exists(path, false) returns null
//      mockZooKeeper.create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT) returns path
//
//      clusterManager ! Connected
//      val ex = (clusterManager ? AddNode(node)).as[ClusterManagerResponse].get.exception
//
//      ex must beNone and (got {
//        one(mockZooKeeper).exists(path, false)
//        one(mockZooKeeper).create(path, node, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
//      })
//    }
//
//    def notifiesForAddNode = this {
//      clusterManager ! Connected
//      (clusterManager ? AddNode(node)).as[Any]
//
//      nodesChangedCount must eventually(be_==(1)) and
//      (nodesReceived.size must be_==(1)) and
//      (nodesReceived must contain(node))
//    }
//
//    def throwsClusterDisconnectedForRemoveNode = {
//      pending
//    }
//
//    def doNothingIfNodeDoesNotExist = {
//      pending
//    }
//
//    def removesNode = {
//      pending
//    }
//
//    def notifiesForRemoveNode = {
//      pending
//    }
//
//    def throwsClusterDisconnectedForMarkAvailable = {
//      pending
//    }
//
//    def addsNodeForAvailable = {
//      pending
//    }
//
//    def doNothingIfAlreadyExists = {
//      pending
//    }
//
//    def notifiesForAvailable = {
//      pending
//    }
//
//    def throwsClusterDisconnectedForMarkUnavailable = {
//      pending
//    }
//
//    def removesNodeForUnavailable = {
//      pending
//    }
//
//    def doNothingIfDoesNotExist = {
////      val path = availabilityNode + "/1"
////
////      mockZooKeeper.exists(path, false) returns mock[Stat]
////
////      //doNothing.when(mockZooKeeper).delete(path, -1)
////
////      clusterManager ! Connected
////      ((clusterManager ? MarkNodeUnavailable(1)).as[ClusterManagerResponse].get.exception must beNone) and
////      (there was one(mockZooKeeper).delete(path, -1))
//      pending
//    }
//
//    def notifiesForUnavailable = this {
//      val membership = new ArrayList[String]
//      membership.add("1")
//      membership.add("2")
//      membership.add("3")
//
//      val availability = membership.clone.asInstanceOf[ArrayList[String]]
//      availability.remove(2)
//
//      val nodes = Array(Node(1, "localhost:31313", true, Set(1, 2)),
//      Node(2, "localhost:31314", false, Set(2, 3)), Node(3, "localhost:31315", true, Set(2, 3)))
//
//      mockZooKeeper.getChildren(membershipNode, true) returns membership
//      nodes.foreach { node =>
//        mockZooKeeper.getData("%s/%d".format(membershipNode, node.id), false, null) returns Node.nodeToByteArray(node)
//      }
//      mockZooKeeper.getChildren(availabilityNode, true) returns availability
//
//      clusterManager ! Connected
//
//      val res1 = (nodesReceived.size must eventually(be_>(0))) //and
////      (nodesReceived.map({ node =>
////        if (node.id == 1) node.available must beTrue
////        else success
////      }).reduce(_ and _))
//
//      (clusterManager ? MarkNodeUnavailable(1)).as[Any]
//      Thread sleep 10
//
////      res1 and (nodesReceived map { node =>
////        if (node.id == 1) node.available must beFalse
////        else success
////      } reduce (_ and _))
//      res1
//    }
//
//  }
//}