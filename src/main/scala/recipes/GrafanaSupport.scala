package recipes

import java.net.{ InetAddress, InetSocketAddress }

trait GrafanaSupport {

  def grafanaInstance = new Grafana {
    override val address = new InetSocketAddress(InetAddress.getByName("192.168.0.182"), 8125)
  }
}
