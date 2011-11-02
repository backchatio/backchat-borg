package backchat.borg
package cluster
package tests

import org.specs2.Specification
import org.specs2.specification.After

class InMemoryClusterClientSpec extends Specification { def is =

  "An InMemoryClusterClient should" ^
    "start with no nodes" ! context.startsWithoutNodes ^
    "add the node" ! context.addsNode ^
    "throw an InvalidNodeException if the node already exists" ! context.throwsInvalidNodeException ^
    "add the node as available" ! context.addNodeAsAvailable ^
    "remove the node" ! context.removesNode ^
    "mark the node available" ! context.marksAvailable ^
    "mark the node unavailable" ! context.marksUnavailable ^
  end
  
  def context = new InMemoryClusterClientSpecContext

  class InMemoryClusterClientSpecContext extends After {
    val clusterClient = new InMemoryClusterClient("test")
    clusterClient.start
    clusterClient.awaitConnectionUninterruptibly
    
    def startsWithoutNodes = this { clusterClient.nodes must beEmpty }
    
    def addsNode = this {
      val r1 = clusterClient.addNode(1, "test") must not beNull

      val nodes = clusterClient.nodes
      val r2 = nodes.size must be_==(1)
      val n = nodes.head
      r1 and r2 and (n.id must_== 1) and (n.url must_== "test") and (n.available must  beFalse)
    }
    
    def throwsInvalidNodeException = this {
      (clusterClient.addNode(1, "test") must not beNull) and 
      (clusterClient.addNode(1, "test") must throwA[InvalidNodeException])
    }
    
    def addNodeAsAvailable = this {
      clusterClient.markNodeAvailable(1)
      clusterClient.addNode(1, "test")
      val nodes = clusterClient.nodes
      nodes.head.available must beTrue
    }

    def removesNode = this {
      clusterClient.addNode(1, "test")
      clusterClient.nodes.size must be_==(1)
      clusterClient.removeNode(1)
      clusterClient.nodes.size must be_==(0)
    }

    def marksAvailable = this {
      clusterClient.addNode(1, "test")
      var nodes = clusterClient.nodes
      val r1 = nodes.head.available must beFalse
      clusterClient.markNodeAvailable(1)
      nodes = clusterClient.nodes
      r1 and (nodes.head.available must beTrue)
    }

    def marksUnavailable = this {
      clusterClient.markNodeAvailable(1)
      clusterClient.addNode(1, "test")
      var nodes = clusterClient.nodes
      val r1 = nodes.head.available must beTrue
      clusterClient.markNodeUnavailable(1)
      nodes = clusterClient.nodes
      r1 and (nodes.head.available must beFalse)    
    }

    def after = {
      clusterClient.shutdown()
    }
  }
}