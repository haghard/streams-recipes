package recipes

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory, ThreadLocalRandom }

import fs2.{ Chunk, Pipe, Pull, Scheduler }
import fs2.async.mutable
import fs2.async.mutable.Queue
import cats.effect.{ Async, Effect, IO }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

//runMain recipes.Fs2Recipes
object Fs2Recipes extends GraphiteSupport with TimeWindows with App {
  val Blocking = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(Fs2Daemons("ts-scheduler")))
  val Main = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, Fs2Daemons("main")))

  def naturalsEvery(latency: Long): fs2.Stream[IO, Long] = {
    def go(i: Long): fs2.Stream[IO, Long] = {
      fs2.Stream.emit(i).flatMap { i ⇒
        fs2.Stream.eval[IO, Long] {
          IO {
            Thread.sleep(latency)
            println(s"produce: $i")
            i
          }
        }
      } ++ go(i + 1)
    }

    go(0l)
  }

  def naturalsEvery2(latency: Long): fs2.Stream[IO, Long] = {
    def go(i: Long): fs2.Stream[IO, Long] = {
      println(s"${Thread.currentThread.getName} - go")
      fs2.Stream.eval {
        for {
          _ ← IO.shift(Blocking)
          n ← IO {
            Thread.sleep(latency)
            val now = System.currentTimeMillis
            println(s"${Thread.currentThread.getName} - ${now}")
            now
          }
          _ ← IO.shift(Main)
        } yield n
      } ++ go(i + 1)
    }
    go(0l)
  }

  //naturalsEvery2(1000).take(10).compile.drain.unsafeRunAsync { _ ⇒ println("done") }

  def takeByOne[F[_], T](n: Int): fs2.Pipe[F, T, T] = {
    def loop(s: fs2.Stream[F, T], n: Int): Pull[F, T, Unit] = {
      if (n <= 0) Pull.done
      else
        s.pull.uncons1.flatMap {
          case None ⇒ Pull.done
          case Some((head, tail)) ⇒
            println(s"emit: $head")
            Pull.output1(head) >> loop(tail, n - 1)
          //Pull.outputChunk(Chunk.vector(Vector.fill(n)(head))) >> loop(tail)
        }
    }
    input ⇒ loop(input, n).stream
  }

  def takeChunks[F[_], T](n: Int): fs2.Pipe[F, T, T] = {
    def loop(s: fs2.Stream[F, T], n: Int): Pull[F, T, Unit] = {
      println(s"loop: $n")
      s.pull.unconsChunk.flatMap {
        case None ⇒ Pull.done
        case Some((head, tail)) ⇒
          val available = head.size
          println(s"chunk available: $available")
          if (available < n) Pull.outputChunk(head) >> loop(tail, n - available)
          else Pull.outputChunk(head.take(n))
      }
    }
    input ⇒ loop(input, n).stream
  }

  //Segments is a bunch of chunks that are being fused together
  def takeSegments[F[_], T](n: Long): fs2.Pipe[F, T, T] = {
    input ⇒
      input.scanSegmentsOpt(n) { n ⇒
        if (n <= 0) None
        else Option { seg ⇒
          seg.take(n).mapResult {
            case Left((_, n)) ⇒ //demanded n more elements
              n
            case Right(_) ⇒ //no elements left
              0
          }
        }
      }
  }

  //fs2.Stream(1,2,3,4,5,5,6,7,8,9,10).through(takeChunks(15)).toVector

  def naturals[F[_]]: Pipe[F, Long, Long] = {
    def next(n: Long): Pull[F, Long, Long] = {
      Thread.sleep(500)
      println(n)
      Pull.output1(n) >> next(n + 1l)
    }

    in ⇒
      in.pull.uncons1.flatMap {
        case None            ⇒ Pull.done
        case Some((head, _)) ⇒ next(head)
      }.stream
  }

  //fs2.Stream(0l).covary[IO].through(naturals).take(10).compile.drain.unsafeRunAsync { _ ⇒ println("done") }

  def stdout[A]: fs2.Pipe[IO, A, Unit] =
    _.evalMap { e ⇒
      IO {
        val delayed = scala.util.Random.nextInt(300)
        Thread.sleep(delayed.toLong)
        println(s"${Thread.currentThread.getName}: latency:$delayed value: $e")
      }
    }

  def stdoutSize[A]: fs2.Pipe[IO, Chunk[A], Unit] = {
    _.evalMap { chunk ⇒
      IO {
        val sleep = scala.util.Random.nextInt(300)
        Thread.sleep(sleep.toLong)
        println(s"[${Thread.currentThread.getName}] latency:$sleep chunk-size:${chunk.toVector.mkString(",")}")
      }
    }
  }

  /*
  val src: Stream[Task, Long] = Stream.emit(-100l)
  (src through (naturals andThen stdout)).run.unsafeRun

  val src1: Stream[Task, Long] = Stream.iterate(Long.MinValue)(_ + 1l)
  (src1 through (dynamicChunks andThen stdoutSize[Long])).run.unsafeRun
  */

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

  //scenario02.compile.drain.unsafeRunSync()
  //scenario03.compile.drain.unsafeRunSync()
  scenario04.compile.drain.unsafeRunSync()

  def logStdOutDelayed[A](message: String): fs2.Pipe[IO, A, Unit] =
    _.evalMap { e ⇒
      IO {
        val delayed = scala.util.Random.nextInt(300)
        println(s"${Thread.currentThread.getName}: start $message: $e")
        Thread.sleep(delayed.toLong)
        println(s"${Thread.currentThread.getName}: stop $message: $e")
      }
    }

  def logStdOut[T]: fs2.Pipe[IO, T, Unit] =
    _.evalMap { n ⇒
      IO(println(s"${Thread.currentThread.getName}: $n"))
    }

  def pipeToGraphite[A](g: GraphiteMetrics, message: String): fs2.Pipe[IO, A, Unit] =
    _.evalMap(_ ⇒ send(g, message))

  def pipeToGraphiteDelayed[A](delay: FiniteDuration)(implicit scheduler: Scheduler, ex: ExecutionContext): fs2.Pipe[IO, A, Unit] =
    _.evalMap { out ⇒
      scheduler.delay(fs2.Stream.eval(IO { out }), delay).compile.drain
    }

  def naturals0(sourceDelay: FiniteDuration, timeWindow: Long, msg: String, monitoring: GraphiteMetrics,
                q: mutable.Queue[IO, Long]): fs2.Stream[IO, Unit] = {
    Scheduler[IO](1).flatMap { scheduler ⇒
      implicit val _ = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(Fs2Daemons("src-scheduler")))
      scheduler.awakeEvery[IO](sourceDelay)
        .take(10000)
        .scan(State(item = 0l)) { (acc, _) ⇒ tumblingWindow(acc, timeWindow) }
        .evalMap { state ⇒ send(monitoring, msg).map(_ ⇒ state.item) }
        .to(q.enqueue)
    }
  }

  def naturals1(sourceDelay: FiniteDuration, timeWindow: Long, msg: String, monitoring: GraphiteMetrics): fs2.Stream[IO, Unit] = {
    Scheduler[IO](1).flatMap { scheduler ⇒
      implicit val _ = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(Fs2Daemons("src-scheduler")))
      scheduler.awakeEvery[IO](sourceDelay)
        .take(10000)
        .scan(State(item = 0l)) { (acc, _) ⇒ tumblingWindow(acc, timeWindow) }
        .evalMap { state ⇒ send(monitoring, msg).map(_ ⇒ state.item) }
    }
  }

  def naturals2(sourceDelay: FiniteDuration, timeWindow: Long, msg: String, monitoring: GraphiteMetrics): fs2.Stream[IO, Long] = {
    Scheduler[IO](1).flatMap { scheduler ⇒
      implicit val _ = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(Fs2Daemons("src-scheduler")))
      scheduler.awakeEvery[IO](sourceDelay)
        .take(10000)
        .scan(State(item = 0l)) { (acc, _) ⇒ tumblingWindow(acc, timeWindow) }
        .map(_.item)
    }
  }

  /*implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)*/
  /*fs2.async.time
      .awakeEvery(sourceDelay)
      .scan(State(item = 0l)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d ⇒ graphite(monitoring, msg).map(_ ⇒ d.item) }
      .to(q.enqueue)*/

  /*def naturals2(sourceDelay: FiniteDuration, timeWindow: Long, msg: String,
                monitoring: GraphiteMetrics): Stream[Task, Long] = {
    val javaScheduler = Executors.newScheduledThreadPool(2, Fs2Daemons("source"))
    implicit val scheduler = fs2.Scheduler.fromScheduledExecutorService(javaScheduler)
    implicit val S = fs2.Strategy.fromExecutor(javaScheduler)
    implicit val Async = Task.asyncInstance(S)
    time
      .awakeEvery(sourceDelay)
      .scan(State(item = 0l)) { (acc, d) ⇒ tumblingWindow(acc, timeWindow) }
      .evalMap { d ⇒ graphite(monitoring, msg).map(_ ⇒ d.item) }
  }*/

  /**
   * Situation:
   * A source and a sink perform on the same rate at the beginning.
   * The sink gets slower, increasing latency with every message.
   * We are using boundedQueue as buffer between the source and the sink.
   * This leads to blocking "enqueue" operation for the source in case no space in the queue.
   * Result:
   * The source's rate is going to decrease proportionally to the sink's rate.
   */
  def scenario02: fs2.Stream[IO, Unit] = {
    val delayPerMsg = 1l
    val window = 5000l
    val bufferSize = 1 << 8
    val sourceDelay = 5.millis

    val srcMessage = "fs2_source_02:1|c"
    val sinkMessage = "fs2_sink_02:1|c"

    val srcG = graphiteInstance
    val sinkG = graphiteInstance

    implicit val _ = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, Fs2Daemons("scenario02")))

    /*fs2.Stream.eval(fs2.async.boundedQueue[IO, Long](bufferSize)).flatMap { q ⇒
      naturals0(sourceDelay, window, srcMessage, srcG, q).mergeHaltL {
        q.dequeue
          .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
          .through(pipeToGraphite[(Long, Long)](sinkG, sinkMessage))
      }
    }
    .handleErrorWith(th ⇒ fs2.Stream.eval(IO(println(s"scenario02 error: ${th.getMessage}"))))
    .onComplete(fs2.Stream.eval(IO(println("scenario02 completed"))))
    */

    (for {
      q ← fs2.Stream.eval(fs2.async.boundedQueue[IO, Long](bufferSize))
      src = naturals0(sourceDelay, window, srcMessage, srcG, q)
      sink = q.dequeue
        .scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
        .through(pipeToGraphite[(Long, Long)](sinkG, sinkMessage))
      out ← src mergeHaltL sink
    } yield out)
      .handleErrorWith(th ⇒ fs2.Stream.eval(IO(println(s"scenario02 error: ${th.getMessage}"))))
      .onComplete(fs2.Stream.eval(IO(println("scenario02 completed"))))
  }

  /**
   * Situation:
   * A source and a sink perform on the same rate in the beginning, later the sink gets slower increasing delay with every message.
   * We are using a separate process that tracks size of a queue, if it reaches the waterMark the top(head) element will be dropped.
   * Result: The source's rate for a long time remains the same (how long depends on waterMark value),
   * but eventually goes down when guard can't keep up anymore, whereas sink's rate goes down immediately.
   *
   *            +-----+
   *      +-----|guard|
   *      |     +-----+
   * +------+   +-----+   +----+
   * |source|---|queue|---|sink|
   * +------+   +-----+   +----+
   *
   *
   *
   */
  def scenario03: fs2.Stream[IO, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 8
    val waterMark = bufferSize - 56 //quarter of buffer size
    val sourceDelay = 10.millis
    val window = 5000l

    val srcMessage = "fs2_source_3:1|c"
    val sinkMessage = "fs2_sink_3:1|c"
    val sinkMessage2 = "fs2_sink2_3:1|c"
    val srcG = graphiteInstance
    val sinkG = graphiteInstance
    val sinkG2 = graphiteInstance
    implicit val _ = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2, Fs2Daemons("scenario03")))

    def dropAll(q: Queue[IO, Long]) =
      fs2.Stream.eval(q.size.get.flatMap(size ⇒ IO((0 to size).foreach(_ ⇒ q.dequeue1))))
        .drain

    def dropQuarter(q: Queue[IO, Long]) =
      fs2.Stream.eval(q.size.get.flatMap(size ⇒ IO((0 to (size / 4)).foreach(_ ⇒ q.dequeue1))))
        .drain

    //just drop element
    def elementDropper(q: Queue[IO, Long]) =
      (q.size.discrete.filter(_ > waterMark) zip dropQuarter(q))
        .through(pipeToGraphite[(Int, Int)](sinkG2, sinkMessage2))

    /*
      fs2.Stream.eval(fs2.async.boundedQueue[IO, Long](bufferSize)).flatMap { q ⇒
        naturals0(sourceDelay, window, srcMessage, srcG, q).mergeHaltL(
          (q.dequeue.scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
            .through(pipeToGraphite(sinkG, sinkMessage))).mergeHaltBoth(overflowGuard(q))
        )
      }
      .handleErrorWith(th ⇒ fs2.Stream.eval(IO(println(s"scenario03 error: ${th.getMessage}"))))
      .onComplete(fs2.Stream.eval(IO(println("scenario03 completed"))))
    */

    (for {
      q ← fs2.Stream.eval(fs2.async.boundedQueue[IO, Long](bufferSize))
      src = naturals0(sourceDelay, window, srcMessage, srcG, q)
      sink = q.dequeue.scan((0l, 0l))((acc, c) ⇒ slowDown(acc, c, delayPerMsg))
        .through(pipeToGraphite(sinkG, sinkMessage))
      dropper = elementDropper(q)
      out ← src mergeHaltL (sink mergeHaltBoth dropper)
    } yield out)
      .handleErrorWith(th ⇒ fs2.Stream.eval(IO(println(s"scenario03 error: ${th.getMessage}"))))
      .onComplete(fs2.Stream.eval(IO(println("scenario03 completed"))))

  }

  /**
   * It always ensures there are 'parallelism' effects being evaluated assuming there's demand for them
   * and they are available from the source stream,
   * whereas the mapAsyncUnordered2 implementation shaded the input in to substreams, so some may not be busy.
   */

  def mapAsyncUnordered[F[_], A, B](parallelism: Int)(f: A ⇒ F[B])(implicit F: Effect[F], ec: ExecutionContext): Pipe[F, A, B] =
    (inner: fs2.Stream[F, A]) ⇒
      inner.map(a ⇒ fs2.Stream.eval(f(a))).join(parallelism)

  def mapAsyncUnordered2[F[_], R](parallelism: Int, stream: fs2.Stream[F, R])(implicit F: Effect[F], ex: ExecutionContext): fs2.Stream[F, R] = {
    val consistentHash = akka.routing.ConsistentHash[Int]((0 to parallelism), 1)
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
  def scenario04: fs2.Stream[IO, Unit] = {
    val window = 5000l
    val parallelism = 2
    val bufferSize = 1 << 8

    val gr = graphiteInstance
    val sourceDelay = 100.millis
    val srcMessage = "fs2_source_4:1|c"

    def sinkMessage(th: String) = s"fs2_sink_${th}:1|c"

    def testSink(e: Long) = IO {
      val rnd = ThreadLocalRandom.current()
      println(s"${Thread.currentThread.getName}: start $e")
      Thread.sleep(rnd.nextInt(100, 300))
      println(s"${Thread.currentThread.getName}: stop $e")
    }

    def testSink0 = IO {
      val rnd = ThreadLocalRandom.current()
      println(s"${Thread.currentThread.getName}: start")
      Thread.sleep(rnd.nextInt(2000, 3000))
      println(s"${Thread.currentThread.getName}: stop")
    }

    import ops._
    implicit val _ = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3, Fs2Daemons("scenario04")))

    def log = fs2.Sink[IO, Long] { in ⇒
      IO {
        println(s"${Thread.currentThread.getName}: start $in")
        Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 2000))
        println(s"${Thread.currentThread.getName}: stop $in")
      }
    }

    for {
      q ← fs2.Stream.eval(fs2.async.boundedQueue[IO, Option[Long]](1 << 8))
      src = Scheduler[IO](1).flatMap {
        implicit val _ = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor(Fs2Daemons("src-scheduler")))
        _.awakeEvery[IO](sourceDelay).take(20).map(v ⇒ Some(v.toMillis)).to(q.enqueue)
          .onComplete(fs2.Stream.eval(q.enqueue1(None).flatMap(_ ⇒ q.enqueue1(None))))
      }
      //.interruptWhen(???)

      sinks = fs2.Stream(
        q.dequeue.unNoneTerminate.to(log),
        q.dequeue.unNoneTerminate.to(log)).join(2)

      //OR
      //ps0 = q.dequeue.unNoneTerminate.to(log)
      //ps1 = q.dequeue.unNoneTerminate.to(log)
      //(ps0 merge ps1).onComplete(fs2.Stream.eval(IO(println("consumers are done")))).drain
      out ← sinks merge src.drain.onComplete(fs2.Stream.eval(IO(println("producer is done")))).drain
    } yield out

    /*naturals1(sourceDelay, window, srcMessage, graphiteInstance)
      .balance[Unit](bufferSize, parallelism)(mapAsyncUnordered[IO, Unit, Unit](parallelism)(_ ⇒ testSink0))
    */
  }

  /*
  //retry the most recently available Task until it passes or until a new Task is available
  val retries = Retries.retryTasksOnFailure(tasks, x ⇒ 1.second)
  retries.run.unsafeRun*/

  //Scalaz-streams to fs2
  //https://gist.github.com/pchlupacek/989a2801036a9441da252726a1b4972d

  //https://gist.github.com/zeryx/5d918a433dca167975ca047a7e1f4cc1
  //combines one model with another, dedups too if text in docA is found in docB using windowSize & number of attempts.

  def combineModels(newModel: fs2.Stream[IO, String], oldModel: fs2.Stream[IO, String],
                    windowSize: Int): fs2.Stream[IO, String] = {
    val doubleDeduped = dedupStreams[String](newModel, oldModel, windowSize)
    val singleDeduped = dedupStream[String](doubleDeduped, windowSize)
    singleDeduped
  }

  def dedupStreams[A](a: fs2.Stream[IO, A], b: fs2.Stream[IO, A], windowSize: Int): fs2.Stream[IO, A] = {
    a.interleaveAll(b).sliding(windowSize).flatMap { window ⇒
      val distinct = window
      println(distinct)
      fs2.Stream.emits(distinct)
    }
  }

  //dedups a single stream based on a window size.
  def dedupStream[A](a: fs2.Stream[IO, A], windowSize: Int): fs2.Stream[IO, A] = {
    a.sliding(windowSize).flatMap { window ⇒
      val distinct = window.distinct
      println(distinct)
      fs2.Stream.emits(distinct)
    }
  }

  /*def circuitBreaker[F[_], O](s1: Stream[F, O], s2: Stream[F, Boolean]): Stream[F, O] = {
     def go(h1: Handle[F, O], h2: Handle[F, Boolean]): Pull[F, O, Nothing] =
       for {
         (c, h2next) <- h2.await1
         (o, h1next) <- if (c) {
           h1.await1
         } else {
           Pull.suspend(go(h1, h2next))
         }
         _ <- Pull.output1(o)
         n <- Pull.suspend(go(h1next, h2next))
       } yield n

     s1.pull2(s2)(go)
  }*/
}
