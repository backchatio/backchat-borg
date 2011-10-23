package backchat
package zeromq


import akka.util.Bootable

trait BootableZeroMqService extends Bootable {
  protected lazy val ioThreads = 1
  override def onUnload {
    ZeroMQ.stop()
  }

  override def onLoad {
    ZeroMQ.start(ioThreads)
  }
}