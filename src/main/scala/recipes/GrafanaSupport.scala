package recipes

import java.net.{ InetAddress, InetSocketAddress }

import fs2.util.Task

trait GrafanaSupport {

  def grafanaInstance = new Grafana {
    override val address = new InetSocketAddress(InetAddress.getByName("192.168.0.182"), 8125)
  }

  def grafana(statsD: Grafana, msg: String): Task[Unit] = Task.delay { statsD send msg }
}
