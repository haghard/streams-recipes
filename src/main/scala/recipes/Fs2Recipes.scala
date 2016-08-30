package recipes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory }

import fs2._
import fs2.Task
import fs2.async.mutable
import fs2.async.mutable.Queue

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

  case class Fs2Daemons(name: String) extends ThreadFactory {
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

  def logGrafana[A](g: Grafana, message: String): fs2.Pipe[Task, A, Unit] =
    _.evalMap { a ⇒ grafana(g, message) }

  def naturals(sourceDelay: FiniteDuration, timeWindow: Long, msg: String, monitoring: Grafana,
               q: mutable.Queue[Task, Long]): Stream[Task, Unit] = {
    val javaScheduler = Executors.newScheduledThreadPool(2, Fs2Daemons("source"))
    implicit val scheduler = fs2.Scheduler.fromScheduledExecutorService(javaScheduler)
    implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)
    time
      .awakeEvery(sourceDelay)
      .scan(State(item = 0l)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d ⇒ grafana(monitoring, msg).map(_ ⇒ d.item) }
      .to(q.enqueue)
  }

  /**
   * Situation:
   * A source and a sink perform on the same rate at the beginning,
   * The sink gets slower, increasing latency with every message.
   * We are using boundedQueue as buffer between the source and the sink.
   * This leads to blocking enqueue operation for the source in case no space in the queue.
   * Result:
   * The source's rate is going to decrease proportionally with the sink's rate.
   */
  def scenario02: Stream[Task, Unit] = {
    val delayPerMsg = 1l
    val window = 5000l
    val bufferSize = 1 << 8
    val sourceDelay = 10.millis

    val srcMessage = "fs2_source_02:1|c"
    val sinkMessage = "fs2_sink_02:1|c"

    val srcG = grafanaInstance
    val sinkG = grafanaInstance
    implicit val qAsync = Task.asyncInstance(
      fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(2, Fs2Daemons("queue"))))

    Stream
      .eval(async.boundedQueue[Task, Long](bufferSize))
      .flatMap { q ⇒
        naturals(sourceDelay, window, srcMessage, srcG, q).mergeDrainL {
          q.dequeue
            .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
            .through(logGrafana[(Long, Long)](sinkG, sinkMessage))
        }
      }
      .onError { ex: Throwable ⇒
        fs2.Stream.eval(Task.now(println(ex.getMessage)))
      }

    /*
    val flow = for {
      q ← Stream.eval(async.boundedQueue[Task, Int](bufferSize))
      out ← naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    } yield out*/

    //flow.evalMap(_ ⇒ grafanaSink(sinkG, sinkMessage)).onError { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
  }

  /**
   *
   * Situation:
   * A source and a sink perform on the same rate in the beginning, later the sink gets slower increasing delay with every message.
   * We are using a separate process that tracks size of queue, if it reaches the waterMark the top element will be dropped.
   * Result: The source's rate for a long time remains the same (how long depends on waterMark value),
   * but eventually goes down when guard can't keep up anymore, whereas sink's rate goes down immediately.
   *
   *                     +-----+
   *              +------|guard|
   *              |      +-----+
   * +------+   +-----+   +----+
   * |source|---|queue|---|sink|
   * +------+   +-----+   +----+
   *
   *
   *
   */
  def scenario03: Stream[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 8
    val waterMark = bufferSize - 56 //quarter of buffer size
    val sourceDelay = 10.millis
    val window = 5000l
    val parallelism = 4

    val srcMessage = "fs2_source_3:1|c"
    val sinkMessage = "fs2_sink_3:1|c"
    val sinkMessage2 = "fs2_sink2_3:1|c"
    val srcG = grafanaInstance
    val sinkG = grafanaInstance
    val sinkG2 = grafanaInstance

    val S = fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(parallelism, Fs2Daemons("queue")))
    implicit val Async = Task.asyncInstance(S)

    def dropAll(q: Queue[Task, Long]) =
      Stream
        .eval(q.size.get.flatMap(size ⇒ Task.traverse((0 to size)) { _ ⇒ q.dequeue1 }))
        .drain

    def dropQuarter(q: Queue[Task, Long]) = {
      val chunk = (0 to waterMark / 4)
      Stream.repeatEval(Task.traverse(chunk) { _ ⇒ q.dequeue1 }.map(_.size))
    }

    def drop(q: Queue[Task, Long]) = q.dequeue

    //just drop element
    def overflowGuard(q: Queue[Task, Long]) =
      (q.size.discrete.filter(_ > waterMark) zip dropQuarter(q)).through(logGrafana[(Int, Int)](sinkG2, sinkMessage2))

    Stream
      .eval(async.boundedQueue[Task, Long](bufferSize)(Async))
      .flatMap { q ⇒
        naturals(sourceDelay, window, srcMessage, srcG, q).mergeDrainL {
          (q.dequeue
            .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
            .through(logGrafana(sinkG, sinkMessage)) mergeHaltBoth overflowGuard(
              q))
        }
      }
      .onError { ex: Throwable ⇒
        Stream.eval(Task.delay(println(s"Error: ${ex.getMessage}")))
      }

    // the same
    /*
    Stream.eval(async.boundedQueue[Task, Long](bufferSize)(Async)).flatMap { q ⇒
      naturals(sourceDelay, window, srcMessage, srcG, q).mergeDrainL {
        concurrent.join(2)(
          Stream[Task, Stream[Task, Unit]](
            overflowGuard(q),
            q.dequeue.scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg)).through(logGrafana(sinkG, sinkMessage))
          )
        )
      }
    }.onError { ex: Throwable ⇒ Stream.eval(Task.delay(println(s"Error: ${ex.getMessage}"))) }
   */
  }
}
