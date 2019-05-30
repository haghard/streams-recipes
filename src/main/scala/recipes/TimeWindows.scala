package recipes

import java.util.concurrent.ThreadLocalRandom

trait TimeWindows {

  private def show(acc: Long, sec: Long) =
    s" ★ ★ ★ ${Thread.currentThread.getName}: count:$acc interval: $sec sec ★ ★ ★ "

  def tumblingWindow(acc: fs.State[Long], timeWindow: Long): fs.State[Long] = {
    if (System.currentTimeMillis - acc.ts > timeWindow) {
      println(show(acc.count, timeWindow / 1000))
      acc.copy(item = acc.item + 1l, ts = System.currentTimeMillis, count = 0l)
    } else acc.copy(item = acc.item + 1l, count = acc.count + 1l)
  }

  def slowDown(accLatency: Long, delayPerMsg: Long): Long = {
    val latency = accLatency + delayPerMsg
    val sleepMillis: Long = 0 + (latency / 1000)
    val sleepNanos: Int = (latency % 1000).toInt
    Thread.sleep(sleepMillis, sleepNanos)

    if (ThreadLocalRandom.current.nextDouble > 0.9995)
      println(s"${Thread.currentThread.getName} Sleep:${sleepMillis} millis and ${sleepNanos} nanos")

    latency
  }
}
