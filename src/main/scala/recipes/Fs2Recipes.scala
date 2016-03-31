package recipes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory }

import fs2.async.mutable
import fs2.util.Task
import fs2._
import scala.concurrent.duration._

/*
  def naturalsEvery(latency: Long): Stream[Task, Int] = {
    def go(i: Int): Stream[Task, Int] =
      Stream.emit(i).flatMap { i ⇒
        fs2.Stream.eval {
          Task.start(Task.delay {
            Thread.sleep(latency)
            i
          }).run
        } ++ go(i + 1)
      }
    go(0)
  }
*/

//runMain recipes.Fs2Recipes
object Fs2Recipes extends GrafanaSupport with TimeWindows with App {
  implicit val scheduler = Executors.newScheduledThreadPool(2, RecipesDaemons("naturals"))
  implicit val qStrategy = fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(2, RecipesDaemons("queue")))

  case class RecipesDaemons(name: String) extends ThreadFactory {
    private def namePrefix = s"$name-thread"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = {
      val t = new Thread(group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      t.setDaemon(true)
      t
    }
  }

  scenario02.runLog.run.run

  def naturals(sourceDelay: Duration, timeWindow: Long,
               msg: String, statsD: Grafana,
               q: mutable.Queue[Task, Int]): Stream[Task, Nothing] =
    time.awakeEvery(sourceDelay)(fs2.Strategy.fromExecutor(scheduler), scheduler)
      .scan(State(item = 0)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap[Task, Unit] { d: State[Int] ⇒ q.enqueue1(d.item).flatMap(_ ⇒ grafanaTask(statsD, msg)) }
      .drain

  /**
   * Situation:
   * A source and a sink perform on the same rate in the beginning, the sink gets slower later increasing latency with every message
   * We are using boundedQueue as buffer between the source and the sink.
   * This leads to blocking enqueue operation for the source in case no space in the queue.
   * Result:
   * The source's rate is going to decrease proportionally with the sink's rate.
   */
  def scenario02: Stream[Task, Unit] = {
    val delayPerMsg = 1l
    val window = 5000l
    val bufferSize = 1 << 7
    val sourceDelay = 10.millis

    val srcMessage = "fs2-source02:1|c"
    val sinkMessage = "fs2-sink02:1|c"

    val srcG = grafanaInstance
    val sinkG = grafanaInstance

    val flow = Stream.eval(async.boundedQueue[Task, Int](bufferSize)).flatMap { q ⇒
      naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    }

    /*
    val flow = for {
      q ← Stream.eval(async.boundedQueue[Task, Int](bufferSize))
      out ← naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    } yield out*/

    flow.evalMap[Task, Unit](_ ⇒ grafanaTask(sinkG, sinkMessage))
      .onError[Task, Unit] { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
  }
}