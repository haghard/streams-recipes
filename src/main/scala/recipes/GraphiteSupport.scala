package recipes

import java.net.{ InetAddress, InetSocketAddress }

import cats.effect.IO

trait GraphiteSupport {

  def graphiteInstance = new GraphiteMetrics {
    override val address = new InetSocketAddress(InetAddress.getByName("localhost"), 8125)
  }

  def send(gr: GraphiteMetrics, msg: String, delay: Long = 0l): IO[Unit] = IO {
    Thread.sleep(delay)
    gr send msg
  }
}
