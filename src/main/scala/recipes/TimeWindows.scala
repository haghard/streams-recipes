package recipes

trait TimeWindows {

  case class State[T](item: T, ts: Long = System.currentTimeMillis(), count: Long = 0)

  private def buildProgress(acc: Long, sec: Long) = s"count:$acc interval:$sec sec"

  def tumblingWindow(acc: State[Int], timeWindow: Long): State[Int] = {
    if (System.currentTimeMillis() - acc.ts > timeWindow) {
      println(buildProgress(acc.count, timeWindow / 1000))
      acc.copy(item = acc.item + 1, ts = System.currentTimeMillis(), count = 0)
    } else acc.copy(item = acc.item + 1, count = acc.count + 1)
  }
}
