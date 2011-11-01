package backchat

import java.net.{ UnknownHostException, SocketException, ConnectException, Socket }
import util.Random

object NetworkUtil {

  def isPortFree(port: Int) = {
    try {
      val socket = new Socket("localhost", port)
      socket.close()
      false
    } catch {
      case e: ConnectException ⇒ true
      case e: SocketException if e.getMessage == "Connection reset by peer" ⇒ true
    }
  }

  private def newPort = Random.nextInt(55365) + 10000

  def randomFreePort(maxRetries: Int = 50) = {
    var count = 0
    var zkPort = newPort
    while (!isPortFree(zkPort)) {
      zkPort = newPort
      count += 1
      if (count >= maxRetries) {
        throw new RuntimeException("Couldn't determine a free port")
      }
    }
    zkPort
  }
}