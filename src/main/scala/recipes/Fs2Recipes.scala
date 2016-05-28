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

  scenario03.run.unsafeAttemptRun

  def naturals(sourceDelay: FiniteDuration, timeWindow: Long,
               msg: String, statsD: Grafana,
               q: mutable.Queue[Task, Int]): Stream[Task, Nothing] = {
    val javaScheduler = Executors.newScheduledThreadPool(2, RecipesDaemons("source"))
    implicit val scheduler = fs2.Scheduler.fromScheduledExecutorService(javaScheduler)
    implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)

    time.awakeEvery(sourceDelay)
      .scan(State(item = 0)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d: State[Int] ⇒ q.enqueue1(d.item).flatMap(_ ⇒ grafanaSink(statsD, msg)) }
      .drain
  }

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

    val srcMessage = "fs2_source_02:1|c"
    val sinkMessage = "fs2_sink_02:1|c"

    val srcG = grafanaInstance
    val sinkG = grafanaInstance
    implicit val qAsync = Task.asyncInstance(fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(2, RecipesDaemons("queue"))))

    val flow = Stream.eval(async.boundedQueue[Task, Int](bufferSize)).flatMap { q ⇒
      naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    }

    /*
    val flow = for {
      q ← Stream.eval(async.boundedQueue[Task, Int](bufferSize))
      out ← naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    } yield out*/

    flow.evalMap(_ ⇒ grafanaSink(sinkG, sinkMessage))
      .onError { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
  }

  /**
   *
   * Situation:
   *  A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
   *  We are using a separate process that tracks size of queue, if it reaches the waterMark the top element will be dropped.
   * Result:
   *  The source's rate stays on original rate but sink's rate goes down.
   *
   *                      +-----+
   *               +------|guard|
   *               |      +-----+
   * +------+   +-----+   +----+
   * |source|---|queue|---|sink|
   * +------+   +-----+   +----+
   *
   * Doesn't work as expected !!!!!
   * source degrades but do it slow
   *
   */
  def scenario03: Stream[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val waterMark = bufferSize - 10
    val sourceDelay = 10.millis
    val window = 5000l
    val parallelism = 6

    val srcMessage = "fs2_source_3:1|c"
    val sinkMessage = "fs2_sink_3:1|c"
    val srcG = grafanaInstance
    val sinkG = grafanaInstance

    val S = fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(parallelism, RecipesDaemons("queue")))
    val Async = Task.asyncInstance(S)

    (Stream eval async.boundedQueue[Task, Int](bufferSize)(Async)).flatMap { q ⇒
      concurrent.join(parallelism)(
        Stream[Task, Stream[Task, Unit]](
          naturals(sourceDelay, window, srcMessage, srcG, q),
          (q.size.discrete.filter(_ > waterMark).evalMap(_ ⇒ q.dequeue1)).drain,
          q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg)).evalMap { _ ⇒ grafanaSink(sinkG, sinkMessage) }
        )
      )(Async)
    }.onError { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }

    /*
    (Stream eval async.boundedQueue[Task, Int](bufferSize)(Async)).flatMap { q ⇒
      wye.merge(
        naturals(sourceDelay, window, srcMessage, srcG, q),
        wye.merge(
          (q.size.discrete.filter(_ > waterMark).evalMap[Task, Int](_ ⇒ q.dequeue1)).drain, /*zip q.dequeue).drain*/
          q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg)).evalMap[Task, Unit] { _ ⇒ grafanaSink(sinkG, sinkMessage) }
        )(Async)
      )(Async)
    }.onError[Task, Unit] { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
    */

  }
}