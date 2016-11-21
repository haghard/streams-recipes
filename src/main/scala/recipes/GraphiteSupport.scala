package recipes

import java.net.{ InetAddress, InetSocketAddress }

import fs2.Task

trait GraphiteSupport {

  def graphiteInstance = new GraphiteMetrics {
    override val address = new InetSocketAddress(InetAddress.getByName("192.168.0.3"), 8125)
  }

  def graphite(gr: GraphiteMetrics, msg: String, delay: Long = 0l): Task[Unit] = Task.delay {
    Thread.sleep(delay)
    gr send msg
  }
}
