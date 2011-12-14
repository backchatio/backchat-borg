package backchat.borg

import java.lang.Thread
import org.multiverse.api.latches.StandardLatch

object KeepAlive {

  private val keepAliveLatch = new StandardLatch
  private var keepAliveThread: Thread = null

  private def keepAlive() {
    keepAliveThread = new Thread(new Runnable {
      def run() {
        try {
          keepAliveLatch.await()
        }
      }
    }, "bc[stayLively]")
    keepAliveThread.setDaemon(false)
    keepAliveThread.start()
  }
  def apply(daemon: Boolean = false)(init: ⇒ Any) {

    if (daemon) System.out.close()
    init
    if (daemon) System.err.close()

    sys.addShutdownHook(keepAliveLatch.open())
    keepAlive()
  }
}