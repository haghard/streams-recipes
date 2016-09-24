package recipes

trait TimeWindows {

  case class State[T](item: T, ts: Long = System.currentTimeMillis, count: Long = 0)

  private def buildProgress(acc: Long, sec: Long) =
    s" ★ ★ ★ count:$acc interval:$sec sec ★ ★ ★ "

  def tumblingWindow(acc: State[Long], timeWindow: Long): State[Long] = {
    if (System.currentTimeMillis - acc.ts > timeWindow) {
      println(buildProgress(acc.count, timeWindow / 1000))
      acc.copy(item = acc.item + 1l, ts = System.currentTimeMillis, count = 0l)
    } else acc.copy(item = acc.item + 1l, count = acc.count + 1l)
  }

  def slowDown(state: (Long, Long), current: Long, delayPerMsg: Long) = {
    val latency = state._1 + delayPerMsg
    Thread.sleep(0 + (latency / 1000), latency % 1000 toInt)
    (latency, current)
  }
}
