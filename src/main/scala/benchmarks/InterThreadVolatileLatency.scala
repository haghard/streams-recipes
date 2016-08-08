package benchmarks

import org.HdrHistogram.Histogram
import scala.annotation.tailrec

//runMain benchmarks.InterThreadVolatileLatency
object InterThreadVolatileLatency {
  val iterations = 100 * 1000 * 1000

  @volatile var ping = -1
  @volatile var pong = -1

  def main(args: Array[String]) = {
    var i = 5
    while (i > 0) {
      i -= 1
      testRun()
    }
  }

  def testRun(): Unit = {
    ping = -1
    pong = -1
    val histogram = new Histogram(3)
    val pongThread = new Thread(new PongRunner())
    val pingThread = new Thread(new PingRunner(histogram))
    pongThread.start()
    pingThread.start()

    pongThread.join()

    println("Histogram of RTT latencies in microseconds")
    histogram.outputPercentileDistribution(System.out, 1000.0)
  }

  class PingRunner(histogram: Histogram) extends Runnable {
    @tailrec final def loop(i: Int, start: Long): Unit = {
      if (i <= iterations) {
        ping = i
        while (i != pong) {
          // busy spin
        }
        histogram.recordValue(System.nanoTime() - start)
        loop(i + 1, System.nanoTime())
      }
    }

    override def run() = loop(0, System.nanoTime())
  }

  class PongRunner extends Runnable {
    @tailrec final def loop(i: Int): Unit = {
      if (i <= iterations) {
        while (i != ping) {
          // busy spin
        }
        pong = i
        loop(i + 1)
      }
    }

    override def run() = loop(0)
  }
}
