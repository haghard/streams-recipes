package benchmarks

import java.util.concurrent.atomic.AtomicBoolean

import org.HdrHistogram.Histogram

import scala.annotation.tailrec

//runMain benchmarks.InterThreadAtomicLatency
object InterThreadAtomicLatency {
  val iterations = 100 * 1000 * 1000
  def main(args: Array[String]) = {
    var i = 5
    while (i > 0) {
      i -= 1
      benchmark()
    }
  }

  def benchmark() {
    val flag = new AtomicBoolean()
    val histogram = new Histogram(3)
    val pingThread = new Thread(new BusySpinCasPingRunner(flag, histogram))
    val pongThread = new Thread(new BusySpinCasPongRunner(flag))

    pongThread.start()
    pingThread.start()
    pongThread.join()

    println("Histogram of RTT latencies in microseconds")
    histogram.outputPercentileDistribution(System.out, 1000.0)
  }

  class BusySpinCasPingRunner(flag: AtomicBoolean, histogram: Histogram) extends Runnable {
    @tailrec final def loop(i: Int, start: Long): Unit = {
      if (i > 0) {
        while (!flag.compareAndSet(false, true)) {
          //busy spin
        }
        histogram.recordValue(System.nanoTime() - start)
        loop(i - 1, System.nanoTime())
      }
    }

    override def run() = loop(iterations, System.nanoTime())
  }

  class BusySpinCasPongRunner(flag: AtomicBoolean) extends Runnable {
    @tailrec final def loop(i: Int): Unit = {
      if (i > 0) {
        while (!flag.compareAndSet(true, false)) {
          // busy spin
        }
        loop(i - 1)
      }
    }

    override def run() = loop(iterations)
  }
}
