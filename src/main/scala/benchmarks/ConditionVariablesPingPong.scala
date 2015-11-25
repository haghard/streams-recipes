package benchmarks

import scala.annotation.tailrec
import org.HdrHistogram.Histogram
import java.util.concurrent.locks.ReentrantLock

//runMain benchmarks.ConditionVariablesPingPong
//Signal between threads using Condition, sleep and wake up threads constantly
object ConditionVariablesPingPong {
  final val iterations = 2 * 1000 * 1000

  val pingLock = new ReentrantLock()
  val pongLock = new ReentrantLock()

  val pingCondition = pingLock.newCondition()
  val pongCondition = pongLock.newCondition()

  var pingValue = -1l
  var pongValue = -1l

  def main(args: Array[String]) = {
    var i = 5
    while (i > 0) {
      i -= 1
      benchmark()
    }
  }

  def benchmark() {
    pingValue = -1l
    pongValue = -1l

    val histogram = new Histogram(3)
    val pingThread = new Thread(new PingRunner(histogram))
    val pongThread = new Thread(new PongRunner())

    pongThread.start()
    pingThread.start()
    pongThread.join()

    println(s"pingValue = $pingValue, pongValue = $pongValue")
    println("Histogram of RTT latencies in microseconds")
    histogram.outputPercentileDistribution(System.out, 1000.0)
  }

  class PingRunner(histogram: Histogram) extends Runnable {
    @tailrec final def loop(i: Int, start: Long): Unit = {
      if (i <= iterations) {
        try {
          pingLock.lock()
          pingValue = i
          pingCondition.signal()
        } finally {
          pingLock.unlock()
        }

        try {
          pongLock.lock()
          while (pongValue != i) {
            pongCondition.await()
          }
        } catch {
          case ex: InterruptedException ⇒ return ()
        } finally {
          pongLock.unlock()
        }

        histogram.recordValue(System.nanoTime() - start)
        loop(i + 1, System.nanoTime())
      } else ()
    }
    override def run() = loop(0, System.nanoTime())
  }

  class PongRunner extends Runnable {
    @tailrec final def loop(i: Int): Unit = {
      if (i <= iterations) {
        try {
          pingLock.lock()
          while (pingValue != i) {
            pingCondition.await()
          }
        } catch {
          case e: InterruptedException ⇒ return ()
        } finally {
          pingLock.unlock()
        }

        try {
          pongLock.lock()
          pongValue = i
          pongCondition.signal()
        } finally {
          pongLock.unlock()
        }
        loop(i + 1)
      } else ()
    }

    override def run() = loop(0)
  }
}
