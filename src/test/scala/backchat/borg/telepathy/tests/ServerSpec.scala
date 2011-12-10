package backchat
package borg
package telepathy
package tests

import mojolly.testing.MojollySpecification

class ServerSpec extends MojollySpecification { def is =
  "A Server should" ^
    "respond with pong when receiving a ping" ! pending ^
    "tracks active client sessions" ! pending ^
    "when receiving a tell message" ^
      controlMessages(null) ^
    "when receiving an ask message" ^
      "reply with the response" ! pending ^
      controlMessages(null) ^
    "when receiving a shout message" ^
      "publish the message to the active subscriptions" ! pending ^
      controlMessages(null) ^
    "when receiving a listen message" ^
      "add the listener to the active subscriptions" ! pending ^
      "add the listener to all active subscriptions" ! pending ^
      "not add the listener to a topic subscription if included in all" ! pending ^
      controlMessages(null) ^
    "when receiving a deafen message" ^
      "remove the listener from the active subscriptions for the specified topics" ! pending ^
      "remove the listener from all active subscriptions" ! pending ^
      controlMessages(null) ^
  end
  
  def controlMessages(req: RequestContext) = {
    "route to the correct handler" ! pending ^
    "do nothing for reliable false" ! pending ^
    "send hug for reliable true" ! pending
  }

  trait RequestContext
}