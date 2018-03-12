package recipes

import java.util.concurrent.ThreadLocalRandom

trait TimeWindows {

  case class State[T](item: T, ts: Long = System.currentTimeMillis, count: Long = 0)

  private def buildProgress(acc: Long, sec: Long) =
    s" ★ ★ ★ ${Thread.currentThread().getName}: count:$acc interval: $sec sec ★ ★ ★ "

  def tumblingWindow(acc: State[Long], timeWindow: Long): State[Long] = {
    if (System.currentTimeMillis - acc.ts > timeWindow) {
      println(buildProgress(acc.count, timeWindow / 1000))
      acc.copy(item = acc.item + 1l, ts = System.currentTimeMillis, count = 0l)
    } else acc.copy(item = acc.item + 1l, count = acc.count + 1l)
  }

  def slowDown(state: (Long, Long), current: Long, delayPerMsg: Long) = {
    val latency = state._1 + delayPerMsg
    val sleepMillis: Long = 0 + (latency / 1000)
    val sleepNanos: Int = (latency % 1000).toInt
    Thread.sleep(sleepMillis, sleepNanos)

    if (ThreadLocalRandom.current().nextDouble() > 0.9995)
      println(s"${Thread.currentThread().getName} Sleep:${sleepMillis} millis and ${sleepNanos} nanos")

    (latency, current)
  }
}
