package recipes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory, ThreadLocalRandom }

import fs2.async.mutable
import fs2.async.mutable.Queue
import fs2.{ Task, _ }

import scala.concurrent.duration._

//runMain recipes.Fs2Recipes
object Fs2Recipes extends GraphiteSupport with TimeWindows with App {

  def naturalsEvery(latency: Long): Stream[Task, Long] = {
    def go(i: Long): Stream[Task, Long] =
      Stream.emit(i).flatMap { i ⇒
        fs2.Stream.eval {
          Task.delay {
            Thread.sleep(latency)
            i
          }
        } ++ go(i + 1)
      }
    go(0l)
  }

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

  def logStdOutDelayed[A](message: String): fs2.Pipe[Task, A, Unit] =
    _.evalMap { e ⇒
      Task.delay {
        val delayed = scala.util.Random.nextInt(300)
        println(s"${Thread.currentThread.getName}: start $message: $e")
        Thread.sleep(delayed.toLong)
        println(s"${Thread.currentThread.getName}: stop $message: $e")
      }
    }

  def logStdOut[T]: fs2.Pipe[Task, T, Unit] =
    _.evalMap { n ⇒
      Task.delay(println(s"${Thread.currentThread.getName}: $n"))
    }

  def logGraphite[A](g: GraphiteMetrics, message: String): fs2.Pipe[Task, A, Unit] =
    _.evalMap { _ ⇒ graphite(g, message) }

  def logGraphiteDelayed[A](delay: FiniteDuration)(implicit strategy: Strategy, scheduler: Scheduler): fs2.Pipe[Task, A, A] =
    _.evalMap { out ⇒
      val delayed = Task.delay(scala.util.Random.nextInt(delay.toMillis.toInt))
      delayed.flatMap(d ⇒ Task.now(out).schedule(d.millis))
    }

  def naturals(sourceDelay: FiniteDuration, timeWindow: Long, msg: String, monitoring: GraphiteMetrics,
               q: mutable.Queue[Task, Long]): Stream[Task, Unit] = {
    val javaScheduler = Executors.newScheduledThreadPool(2, Fs2Daemons("source"))
    implicit val scheduler = fs2.Scheduler.fromScheduledExecutorService(javaScheduler)
    implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)
    time
      .awakeEvery(sourceDelay)
      .scan(State(item = 0l)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d ⇒ graphite(monitoring, msg).map(_ ⇒ d.item) }
      .to(q.enqueue)
  }

  def naturals2(sourceDelay: FiniteDuration, timeWindow: Long, msg: String,
                monitoring: GraphiteMetrics): Stream[Task, Long] = {
    val javaScheduler = Executors.newScheduledThreadPool(2, Fs2Daemons("source"))
    implicit val scheduler = fs2.Scheduler.fromScheduledExecutorService(javaScheduler)
    implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)
    time
      .awakeEvery(sourceDelay)
      .scan(State(item = 0l)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d ⇒ graphite(monitoring, msg).map(_ ⇒ d.item) }
  }

  /**
   * Situation:
   * A source and a sink perform on the same rate at the beginning,
   * The sink gets slower, increasing latency with every message.
   * We are using boundedQueue as buffer between the source and the sink.
   * This leads to blocking "enqueue" operation for the source in case no space in the queue.
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

    val srcG = graphiteInstance
    val sinkG = graphiteInstance
    implicit val qAsync = Task.asyncInstance(fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(2, Fs2Daemons("queue"))))

    Stream.eval(async.boundedQueue[Task, Long](bufferSize))
      .flatMap { q ⇒
        naturals(sourceDelay, window, srcMessage, srcG, q).mergeDrainL {
          q.dequeue
            .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
            .through(logGraphite[(Long, Long)](sinkG, sinkMessage))
        }
      }
      .onError { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }

    /*
    val flow = for {
      q ← Stream.eval(async.boundedQueue[Task, Int](bufferSize))
      out ← naturals(sourceDelay, window, srcMessage, srcG, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ injectLatency(acc, c, delayPerMsg))
    } yield out*/

    //flow.evalMap(_ ⇒ grafanaSink(sinkG, sinkMessage)).onError { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
  }

  /**
   * Situation:
   * A source and a sink perform on the same rate in the beginning, later the sink gets slower increasing delay with every message.
   * We are using a separate process that tracks size of a queue, if it reaches the waterMark the top(head) element will be dropped.
   * Result: The source's rate for a long time remains the same (how long depends on waterMark value),
   * but eventually goes down when guard can't keep up anymore, whereas sink's rate goes down immediately.
   *
   *        +-----+
   * +------|guard|
   * |      +-----+
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
    val srcG = graphiteInstance
    val sinkG = graphiteInstance
    val sinkG2 = graphiteInstance

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
      (q.size.discrete.filter(_ > waterMark) zip dropQuarter(q)).through(logGraphite[(Int, Int)](sinkG2, sinkMessage2))

    Stream
      .eval(async.boundedQueue[Task, Long](bufferSize)(Async))
      .flatMap { q ⇒
        naturals(sourceDelay, window, srcMessage, srcG, q).mergeDrainL {
          (q.dequeue
            .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
            .through(logGraphite(sinkG, sinkMessage)) mergeHaltBoth overflowGuard(q))
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
            q.dequeue.scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg)).through(logGraphite(sinkG, sinkMessage))
          )
        )
      }
    }.onError { ex: Throwable ⇒ Stream.eval(Task.delay(println(s"Error: ${ex.getMessage}"))) }
   */
  }

  /**
   * It always ensures there are 'parallelism' effects being evaluated assuming there's demand for them
   * and they are available from the source stream,
   * whereas the mapAsyncUnordered2 implementation shaded the input in to substreams, so some may not be busy.
   */
  def mapAsyncUnordered[F[_]: fs2.util.Async, A, B](parallelism: Int)(f: A ⇒ F[B]): Pipe[F, A, B] =
    (inner: Stream[F, A]) ⇒
      concurrent.join(parallelism)(inner.map(a ⇒ Stream.eval(f(a))))

  def mapAsyncUnordered2[F[_], R](parallelism: Int)(stream: Stream[F, R])(implicit F: fs2.util.Async[F]): Stream[F, R] = {
    val consistentHash = akka.routing.ConsistentHash[Int]((0 to parallelism), 1)
    //consistentHash.nodeFor(value.hashCode.toString)
    if (parallelism <= 1) stream
    else {
      val zeroStream = stream.filter { value ⇒ consistentHash.nodeFor(value.hashCode.toString) % parallelism == 0 }
      (1 to (parallelism - 1)).foldLeft(zeroStream) {
        case (s, num) ⇒
          val filtered = s.filter { value ⇒ consistentHash.nodeFor(value.hashCode.toString) % parallelism == num }
          s.merge(filtered)
      }
    }
  }

  implicit class StreamOps[F[_], A](val source: Stream[F, A]) extends AnyVal {
    def balance[B](qSize: Int, parallelism: Int)(sink: Pipe[F, A, B], qSizeSink: Pipe[F, Int, Unit])(implicit a: fs2.util.Async[F]): Stream[F, B] = {
      Stream.eval(async.boundedQueue[F, Option[A]](qSize)).flatMap { q ⇒
        source.map(Some(_)).to(q.enqueue)
          //.evalMap { el ⇒ asc.flatMap(q.enqueue1(el)) { r ⇒ println(q.hashCode); asc.pure(()) } }
          .drain.onFinalize[F] {
            def close(n: Int): F[Unit] =
              if (n == 1) q.enqueue1(None) else a.flatMap(q.enqueue1(None))(_ ⇒ close(n - 1))

            close(parallelism)
            //a.flatMap(q.enqueue1(None)) { r ⇒ println("Source is done"); a.pure(()) }
          }
          //.mergeHaltBoth(q.size.discrete.through(qSizeSink).drain)
          .merge(q.dequeue.unNoneTerminate.through(sink))

      }
    }
  }

  /**
   * 2 sinks run in parallel to balance the load
   * Source throughput == throughput_Sink1 + throughput_Sink2
   *
   *                             +-----+
   *                      +------|sink0|
   * +------+   +-----+   |      +-----+
   * |source|---|queue|---|
   * +------+   +-----+   |      +-----+
   *                      +------|sink1|
   *                             +-----+
   */
  def scenario04: Stream[Task, Unit] = {
    val window = 5000l
    val parallelism = 2
    val bufferSize = 1 << 8

    val gr = graphiteInstance
    val sourceDelay = 200.millis
    val srcMessage = "fs2_source_4:1|c"
    implicit val Async = Task.asyncInstance(
      fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(parallelism + 1, Fs2Daemons("sinks"))))

    def sinkMessage(th: String) = s"fs2_sink_${th}:1|c"

    def testSink(e: Long) = Task.delay {
      val rnd = ThreadLocalRandom.current()
      println(s"${Thread.currentThread.getName}: start $e")
      Thread.sleep(rnd.nextInt(100, 300))
      println(s"${Thread.currentThread.getName}: stop $e")
    }

    naturals2(sourceDelay, window, srcMessage, graphiteInstance) //.take(100)
      .balance(bufferSize, parallelism)(
        mapAsyncUnordered(parallelism) { e ⇒
          graphite(gr, sinkMessage(Thread.currentThread.getName), 300)
          //testSink(e)
        }, logStdOut
      ).onError { ex: Throwable ⇒
          Stream.eval(Task.delay(println(s"fs2_scenario04 error: ${ex.getMessage}")))
        }
  }

  //Task.fromFuture()
  /*def go(out: FileHandle[Task]): Handle[Task,MyEvent] => Pull[Task,Nothing,Unit] =
    _.receive1 {
      case (MyEvent.Data(d), h) => Pull.eval(out.write(data)) >> go(out)(h)
      case (MyEvent.NewFile(name), h) => Pull.eval(out.close) >> io.file.pulls.fromPathAsync[Task](name, ...).flatMap { newOut => go(newOut)(h) }
    }*/
}
