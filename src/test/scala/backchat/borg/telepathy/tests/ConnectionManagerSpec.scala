package backchat.borg
package telepathy
package tests

import mojolly.testing.MojollySpecification
import telepathy.ConnectionManager._

class ConnectionManagerSpec extends MojollySpecification { def is =

  "A ConnectionManager should" ^
    "connect to the available servers" ! specify.connectsToAvailableServers ^
    "not be alive when there are no available servers" ! specify.isNotAlive ^
    "be alive when there are available servers" ! specify.isAlive ^ bt ^
    "when picking a server for a request" ^
      "pick any server for an inactive service" ! specify.picksAnyForInactiveService ^
      "pick the server providing an already active service" ! specify.picksProvidingServerForActiveService ^ bt ^
    "clean up when closed" ! specify.cleansUpWhenClosed ^
    "return which servers need a ping" ! specify.returnsServersThatNeedAPing ^
    "expire the servers" ! specify.expiresServers ^
    "when updating the available servers" ^
      "remove the active servers for removed available servers" ! specify.removesActiveServers ^
      "notify listeners of active server removal" ! specify.notifiesActiveServerRemoval ^
      "add active servers for active services" ! specify.addsActiveServers ^
      "notify listeners of new active services" ! specify.notifiesActiveServerAddition ^
      "not be alive when there are no more available servers" ! specify.isNotAliveWhenNoMoreAvailableServers ^
  end

  def specify = new ConnectionManagerSpecContext

  class ConnectionManagerSpecContext {

    def connectsToAvailableServers = pending
    
    def isNotAlive = ConnectionManager().isAlive must beFalse
    def isAlive = ConnectionManager(AvailableServers("first" -> AvailableServer("ipc://nowhere"))).isAlive must beTrue

    def picksAnyForInactiveService = pending
    
    def picksProvidingServerForActiveService = pending
    
    def cleansUpWhenClosed = {
      val cm = ConnectionManager(AvailableServers("first" -> AvailableServer("ipc://nowhere")))
      cm.close
      val emptyAsserts = (cm.availableServers :: cm.activeServers :: Nil) map (_ must beEmpty)
      emptyAsserts reduce (_ and _) and (cm.isAlive must beFalse)
    }
    
    def returnsServersThatNeedAPing = pending
    
    def expiresServers = pending

    def removesActiveServers = pending

    def notifiesActiveServerRemoval = pending

    def addsActiveServers = pending

    def notifiesActiveServerAddition = pending

    def isNotAliveWhenNoMoreAvailableServers = pending
  }
}