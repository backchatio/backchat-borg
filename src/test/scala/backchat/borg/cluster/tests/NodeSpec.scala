package backchat.borg
package cluster
package tests

import org.specs2.Specification
import backchat.borg.protos.BorgProtos

class NodeSpec extends Specification { def is =

  "A Node should" ^
    "serialize into the correct format" ! serializeCorrectly ^
    "deserialize into the correct Node" ! deserializeCorrectly ^
    "be equal to another node if they have the same id and url" ! equalsOnIdAndUrl ^
    "be different when the id's are different" ! differsOnId ^
    "be different when the url's are different" ! differsOnUrl ^
    "be equal to itself" ! context.equalsToSelf ^
    "equals symmetrically" ! context.equalsSymmetric ^
    "equals transitively" ! context.equalsTransitive ^
    "equals with a null" ! context.handlesNull ^
    "has the same hashcodes for equal nodes with different extra content" ! context.checkHashCodes ^ end
  
  def serializeCorrectly = {
    val builder = BorgProtos.Node.newBuilder
    builder.setId(1)
    builder.setUrl("localhost:31313")
    builder.addPartition(0).addPartition(1)
    val expectedBytes = builder.build.toByteArray.toList

    val nodeBytes = Node.nodeToByteArray(Node(1, "localhost:31313", false, Set(0, 1))).toList
    nodeBytes must be_==(expectedBytes)
  }

  def deserializeCorrectly = {
    val builder = BorgProtos.Node.newBuilder
    builder.setId(1)
    builder.setUrl("localhost:31313")
    builder.addPartition(0).addPartition(1)
    val bytes = builder.build.toByteArray

    val node = Node(1, "localhost:31313", true, Set(0, 1))
    Node(1, bytes, true) must be_==(node)
  }

  def equalsOnIdAndUrl = {
    val node1 = Node(1, context.url, true, Set(0, 1))
    val node2 = Node(1, context.url, false, Set(1, 2))
    node1 must_== node2
  }

  def differsOnId = {
    val node1 = Node(1, context.url, true, Set(0, 1))
    val node2 = Node(2, context.url, false, Set(1, 2))
    node1 must_!= node2
  }

  def differsOnUrl = {
    val node1 = Node(1, context.url, true, Set(0, 1))
    val node2 = Node(1, "aaa" + context.url, false, Set(1, 2))
    node1 must_!= node2
  }
  
  object context {
    val url = "localhost:31313"
    val node1 = Node(1, url, true, Set(0, 1))
    val node2 = Node(1, url, false, Set(2, 3))
    val node3 = Node(1, url, true, Set(4, 5))
    
    def equalsToSelf = node1 must_== node1
    def equalsSymmetric = (node1 must_== node2) and (node2 must_== node1)
    def equalsTransitive = (node1 must_== node2) and (node2 must_== node3) and (node3 must_== node1)
    def handlesNull = node1 must_!= null
    def checkHashCodes = node1.hashCode must_== node2.hashCode
  }
}