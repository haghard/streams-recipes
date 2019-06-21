package recipes

import java.io.FileInputStream
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ForkJoinPool, ThreadFactory}

import scodec.bits.ByteVector

import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Random
import scalaz.stream.Process._
import scalaz.{-\/, \/, \/-, Nondeterminism}
import scalaz.stream._
import scalaz.stream.merge._
import scalaz.concurrent.{Strategy, Task}
import scala.concurrent.duration._

///runMain recipes.ScalazRecipes
object ScalazRecipes extends App {
  val showLimit     = 1000
  val observePeriod = 5000
  val limit         = Int.MaxValue
  val statsD        = new InetSocketAddress(InetAddress.getByName("192.168.77.83"), 8125)
  val Ex            = Strategy.Executor(new ForkJoinPool(Runtime.getRuntime.availableProcessors))

  case class RecipesDaemons(name: String) extends ThreadFactory {
    private def namePrefix         = s"$name-thread"
    private val threadNumber       = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup
    override def newThread(r: Runnable) = {
      val t = new Thread(group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      t.setDaemon(true)
      t
    }
  }

  def graphiteInstance = new GraphiteMetrics { override val address = statsD }

  //scenario07.run[Task].unsafePerformSync
  scenario08.run[Task].unsafePerformSync

  def naturalsEvery(latency: Long): Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.delay { Thread.sleep(latency); i })(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }

  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.now(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }

  def rnd: Process[Task, Int] = {
    def go(rnd: java.util.concurrent.ThreadLocalRandom): Process[Task, Int] =
      Process.await(Task.now(rnd))(rnd ⇒ Process.emit(rnd.nextInt(1, 100)) ++ go(rnd))
    go(java.util.concurrent.ThreadLocalRandom.current)
  }

  def graphite(statsD: GraphiteMetrics, message: String) = sink.lift[Task, Int] { _ ⇒
    Task.delay(statsD send message)
  }

  def graphiteS(s: scalaz.stream.async.mutable.Signal[Int], statsD: GraphiteMetrics, message: String) =
    sink.lift[Task, Int] { x: Int ⇒
      s.set(x).map(_ ⇒ statsD send message)
    }

  def broadcastN[T](n: Int, source: Process[Task, T], bufferSize: Int = 4)(
    implicit S: Strategy
  ): Process[Task, Seq[Process[Task, T]]] = {
    val queues = (0 until n).map(_ ⇒ async.boundedQueue[T](bufferSize)(S))
    val broadcast = queues
      ./:(source)((src, q) ⇒ (src observe q.enqueue))
      .onComplete(Process.eval(Task.gatherUnordered(queues.map(_.close))))
    (broadcast.drain merge Process.emit(queues.map(_.dequeue)))(S)
  }

  def broadcastManyBounded[T](n: Int, src: Process[Task, T], waterMark: Int, bufferSize: Int = 4)(
    implicit S: Strategy
  ): Process[Task, Seq[Process[Task, T]]] = {
    val queues = (0 until n).map(_ ⇒ async.boundedQueue[T](bufferSize)(S))
    val broadcast = queues
      ./:(src) { (src, q) ⇒
        ((q.size.discrete.filter(_ > waterMark) zip q.dequeue).drain merge src.observe(q.enqueue))
      }
      .onComplete(Process.eval(Task.gatherUnordered(queues.map(_.close))))
    (broadcast.drain merge Process.emit(queues.map(_.dequeue)))(S)
  }

  def interleaveN[T](q: scalaz.stream.async.mutable.Queue[T], processes: List[Process[Task, T]])(
    implicit S: Strategy
  ): Process[Task, T] = {
    val merge = processes.tail
      ./:(processes.head to q.enqueue) { (acc: Process[Task, Unit], p: Process[Task, T]) ⇒
        (acc merge (p to q.enqueue))(S)
      }
      .onComplete(Process.eval(q.close))
    (merge.drain merge q.dequeue)(S)
  }

  implicit class SinkOps[A](val self: Sink[Task, A]) extends AnyVal {
    def nondeterminstically(right: Sink[Task, A]) =
      (self zipWith right) { (l, r) ⇒ (a: A) ⇒
        Nondeterminism[Task].mapBoth(l(a), r(a))((_, _) ⇒ ())
      }
  }

  /**
    * What is time series data ?
    *
    * Measurements taken at regular intervals and each measurement has ts attached to it.
    * If we have a log from some production system so we have lines with ts attached to it
    * it isn't a time series data.
    * So time series data is a continuous measurement at a regular interval
    *
    * Of cause you can turn lines with ts into time series
    *
    *
    * TimeSeries:
    *   TumblingWindow: discretize a stream into non-overlapping windows
    *   SlidingWindow: discretize a stream into overlapping windows
    *
    */
  implicit class TimeSeriesProcessesOps[T](val p: Process[Task, T]) extends AnyVal {
    import scalaz.stream.ReceiveY.{HaltOne, ReceiveL, ReceiveR}

    /**
      * Count window
      */
    def countWindow(aggregateInterval: Duration)(implicit S: scalaz.concurrent.Strategy): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis) wye p)(tumblingWye[T](aggregateInterval, false))(S)

    /**
      * Tumbling windows discretize a stream into non-overlapping windows
      */
    def tumblingWindow(aggregateInterval: Duration)(implicit S: scalaz.concurrent.Strategy): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis) wye p)(tumblingWye[T](aggregateInterval))(S)

    /**
      * Sliding windows discretize a stream into overlapping windows
      */
    def slidingWindow(aggregateInterval: Duration, numOfUnits: Int)(
      implicit S: scalaz.concurrent.Strategy
    ): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis / numOfUnits) wye p)(tumblingWye[T](aggregateInterval))(S)

    private def tumblingWye[I](duration: Duration, reset: Boolean = true): scalaz.stream.Wye[Long, I, I] = {
      val timeWindow = duration.toNanos
      val P          = scalaz.stream.Process
      val nano       = 1000000000
      def go(acc: Long, last: Long, n: Int): Wye[Long, I, I] =
        P.awaitBoth[Long, I].flatMap {
          case ReceiveL(currentNanos) ⇒
            if (currentNanos - last > timeWindow) {
              println(buildProgress(n, acc, (currentNanos - last) / nano))
              go(if (reset) 0L else acc + 1L, currentNanos, 1)
            } else {
              println(buildProgress(n, acc, (currentNanos - last) / nano))
              go(acc, last, n + 1)
            }
          case ReceiveR(i) ⇒ P.emit(i) ++ go(acc + 1L, last, n)
          case HaltOne(e)  ⇒ P.Halt(e)
        }

      go(0L, System.nanoTime, 1)
    }

    private def buildProgress(i: Int, acc: Long, sec: Long) =
      s"${List.fill(i)(" ★ ").mkString} number:$acc interval: $sec sec"

    private def discreteStep(millis: Long) =
      Process.repeatEval(Task.delay { Thread.sleep(millis); System.nanoTime })
  }

  /**
    * Situation: A source and a sink perform on the same rates.
    * Result: The source and the sink are going on the same rate.
    */
  def scenario01: Process[Task, Unit] = {
    val sourceDelay       = 20L
    val latency: Duration = 25 seconds
    val srcMessage        = "scalaz-source1:1|c"
    val sinkMessage       = "scalaz-sink1:1|c"

    val src = (naturalsEvery(sourceDelay) observe graphite(graphiteInstance, srcMessage))
    (src tumblingWindow latency)(Ex) to graphite(graphiteInstance, sinkMessage)
    //(src.throughSlidingWindow(latency, 5))(Ex) to statsDin(statsDInstance, sinkMessage)
    //(src throughAllWindow latency)(Ex) to statsDin(statsDInstance, sinkMessage)
  }

  /**
    * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increasing delay with every message.
    * We are using boundedQueue as buffer between them, which leads to blocking the source in case no space in the queue.
    * Result: The source's rate is going to decrease proportionally with the sink's rate.
    */
  def scenario02: Process[Task, Unit] = {
    val delayPerMsg               = 1L
    val bufferSize                = 1 << 7
    val sourceDelay               = 10
    val triggerInterval: Duration = 5 seconds
    val srcMessage                = "scalaz-source2:1|c"
    val sinkMessage               = "scalaz-sink2:1|c"
    val queue                     = async.boundedQueue[Int](bufferSize)(Ex)

    (naturalsEvery(sourceDelay) observe queue.enqueue to graphite(graphiteInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run
      .unsafePerformAsync(_ ⇒ ())

    (queue.dequeue.stateScan(0L)(
      { v: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          updated = latency + delayPerMsg
          _       = Thread.sleep(0 + (updated / 1000), updated % 1000 toInt)
          _ ← scalaz.State.put(updated)
        } yield v
      }
    ) slidingWindow (triggerInterval, 5))(Ex) to graphite(graphiteInstance, sinkMessage)
  }

  /**
    * Situation:
    *  A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
    *  We are using circular buffer as buffer between them, it will override elements if no space in the buffer.
    * Result:
    *  The sink's rate is going to be decreased but the source's rate stays on the initial level.
    */
  def scenario03: Process[Task, Unit] = {
    val delayPerMsg      = 1L
    val bufferSize       = 1 << 7
    val sourceDelay      = 20
    val window: Duration = 5 seconds
    val cBuffer          = async.circularBuffer[Int](bufferSize)(Ex)

    val srcMessage  = "scalaz-source3:1|c"
    val sinkMessage = "scalaz-sink3:1|c"

    (naturalsEvery(sourceDelay) observe cBuffer.enqueue to graphite(graphiteInstance, srcMessage))
      .onComplete(Process.eval_(cBuffer.close))
      .run
      .unsafePerformAsync(_ ⇒ ())

    (cBuffer.dequeue.stateScan(0L) { n: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        updated = latency + delayPerMsg
        _       = Thread.sleep(0 + (updated / 1000), updated % 1000 toInt)
        _ ← scalaz.State.put(updated)
      } yield n
    } tumblingWindow window)(Ex) to graphite(graphiteInstance, sinkMessage)
  }

  /**
    * This behaves like scenario03. The only difference being that we are using queue instead of circular buffer
    *
    *
    *                      +--------+
    *               +------|dropLast|
    *               |      +--------+
    * +------+   +-----+   +----+
    * |source|---|queue|---|sink|
    * +------+   +-----+   +----+
    */
  def scenario03_1: Process[Task, Unit] = {
    val delayPerMsg      = 1L
    val bufferSize       = 1 << 7
    val waterMark        = bufferSize - 5
    val sourceDelay      = 10
    val window: Duration = 5 seconds
    val queue            = async.boundedQueue[Int](bufferSize)(Ex)

    val srcMessage  = "scalaz-source3_1:1|c"
    val sinkMessage = "scalaz-sink3_1:1|c"

    def dropLastCleaner =
      (queue.size.discrete.filter(_ > waterMark) zip queue.dequeue).drain

    ((naturalsEvery(sourceDelay) tumblingWindow window) observe queue.enqueue to graphite(graphiteInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run
      .unsafePerformAsync(_ ⇒ ())

    val qSink = (queue.dequeue.stateScan(0L) { number: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          increased = latency + delayPerMsg
          _         = Thread.sleep(0 + (increased / 1000), increased % 1000 toInt)
          _ ← scalaz.State.put(increased)
        } yield number
      } tumblingWindow window)(Ex) to graphite(graphiteInstance, sinkMessage)

    mergeN(Process(qSink, dropLastCleaner))(Ex)
  }

  /**
    * This one is different from scenario03_1 only in dropping the whole buffer when waterMark is reached
    *                      +----------+
    *               +------|dropBuffer|
    *               |      +----------+
    * +------+   +-----+   +----+
    * |source|---|queue|---|sink|
    * +------+   +-----+   +----+
    *
    */
  def scenario03_2: Process[Task, Unit] = {
    val delayPerMsg      = 1L
    val bufferSize       = 1 << 7
    val waterMark        = bufferSize - 5
    val sourceDelay      = 10
    val window: Duration = 5 seconds
    val queue            = async.boundedQueue[Int](bufferSize)(Ex)

    val srcMessage  = "scalaz-source3_2:1|c"
    val sinkMessage = "scalaz-sink3_2:1|c"

    val dropBufferProcess = (queue.size.discrete
      .filter(_ > waterMark) zip (queue dequeueBatch waterMark)).drain

    ((naturalsEvery(sourceDelay) tumblingWindow window) observe queue.enqueue to graphite(graphiteInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run[Task]
      .unsafePerformAsync(_ ⇒ ())

    val sink = (queue.dequeue.stateScan(0L) { ind: Int ⇒
        for {
          currentLatency ← scalaz.State.get[Long]
          increased = currentLatency + delayPerMsg
          _         = Thread.sleep(0 + (increased / 1000), increased % 1000 toInt)
          _ ← scalaz.State.put(increased)
        } yield ind
      } tumblingWindow window)(Ex) to graphite(graphiteInstance, sinkMessage)

    mergeN(Process(sink, dropBufferProcess))(Ex)
  }

  /**
    * A fast source do broadcast into two sinks. The first sink is fast and the second is getting slower.
    * Result: The whole flow rate is going to be decreased up to slow sink.
    *
    *                +------+  +-----+
    *             +--|queue0|--|sink0|
    * +-------+   |  +------+  +-----+
    * |source0|---|
    * +-------+   |  +------+  +-----+
    *             +--|queue1|--|sink1|
    *                +------+  +-----+
    */
  def scenario04: Process[Task, Unit] = {
    val delayPerMsg  = 1L
    val window       = 5 seconds
    val producerRate = 10
    val srcMessage   = "scalaz-source_4:1|c"
    val sinkMessage0 = "scalaz-sink_4_1:1|c"
    val sinkMessage1 = "scalaz-sink_4_2:1|c"

    val src = (naturalsEvery(producerRate) tumblingWindow window) observe graphite(graphiteInstance, srcMessage)

    (for {
      outlets ← broadcastN(2, src)(Ex)

      out0 = outlets(0) to graphite(graphiteInstance, sinkMessage0)

      out1 = outlets(1).stateScan(0L) { number: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          increased = latency + delayPerMsg
          _         = Thread.sleep(0 + (increased / 1000), increased % 1000 toInt)
          _ ← scalaz.State.put(increased)
        } yield number
      } to graphite(graphiteInstance, sinkMessage1)

      _ ← (out0 merge out1)(Ex)
    } yield ())
  }

  /**
    * A fast source do broadcast into two sinks. The first sink is fast and the second is getting slower.
    * We use drop strategy when waterMark is exceeded
    * Result: source and fast sink's rate stay the same, slow sink is going down
    *
    *                    +----- drop
    *                    |
    *                +------+  +-----+
    *             +--|queue0|--|sink0|
    * +-------+   |  +------+  +-----+
    * |source0|---|
    * +-------+   |  +------+  +-----+
    *             +--|queue1|--|sink1|
    *                +------+  +-----+
    *                    |
    *                    +----- drop
    */
  def scenario05: Process[Task, Unit] = {
    val delayPerMsg  = 1L
    val sourceDelay  = 10
    val window       = 5 seconds
    val bufferSize   = 1 << 7
    val waterMark    = bufferSize - 5
    val srcMessage   = "scalaz-source5:1|c"
    val sinkMessage0 = "scalaz-sink5_0:1|c"
    val sinkMessage1 = "scalaz-sink5_1:1|c"

    val src = (naturalsEvery(sourceDelay) tumblingWindow window) observe graphite(graphiteInstance, srcMessage)

    (for {
      outlets ← broadcastManyBounded(2, src, waterMark, bufferSize)(Ex)

      out0 = outlets(0) to graphite(graphiteInstance, sinkMessage0)

      out1 = outlets(1).stateScan(0L) { number: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          increased = latency + delayPerMsg
          _         = Thread.sleep(0 + (increased / 1000), increased % 1000 toInt)
          _ ← scalaz.State.put(increased)
        } yield number
      } to graphite(graphiteInstance, sinkMessage1)

      _ ← (out0 merge out1)(Ex)
    } yield ())
  }

  /**
    * Situation: Multiple sources operating on different rates have merged into one sink
    * Sink's rate = sum(sources)
    *
    * +----+
    * |src0|-+
    * +----+ |
    * +----+ | +------------+  +----+
    * |src1|---|boundedQueue|--|sink|
    * +----+ | +------------+  +----+
    * +----+ |
    * |src2|-+
    * +----+
    *
    */
  def scenario07: Process[Task, Unit] = {
    val window             = 10 seconds
    val latencies          = List(20L, 30L, 40L, 45L)
    def srcMessage(n: Int) = s"scalaz-source7_$n:1|c"

    val sources = latencies.zipWithIndex.map { ms ⇒
      naturalsEvery(ms._1) observe graphite(graphiteInstance, srcMessage(ms._2))
    }

    (interleaveN(async.boundedQueue[Int](1 << 8)(Ex), sources)(Ex) slidingWindow (window, 5)) to graphite(
      graphiteInstance,
      "scalaz-sink7:1|c"
    )
  }

  /**
    * Usage:
    * val src: Process[Task, Char] = Process.emitAll(Seq('a', 'b', 'c', 'd','e'))
    * (src |> count).runLog.run
    * Vector(-\/(1), \/-(a), -\/(2), \/-(b), -\/(3), \/-(c), -\/(4), \/-(d), -\/(5), \/-(e))
    *
    */
  def count[A]: Process1[A, Long \/ A] = {
    def go(acc: Long): Process1[A, Long \/ A] =
      Process.receive1[A, Long \/ A] { element: A ⇒
        Process.emitAll(Seq(-\/(acc), \/-(element))) ++ go(acc + 1)
      }
    go(1L)
  }

  /**
    * A Tee that drop from left while the predicate `p` is true for the
    * values, then continue with the Tee `rest`
    *
    */
  def dropWhileL[L, R, O](p: L ⇒ Boolean)(rest: Tee[L, R, O]): Tee[L, R, O] =
    Process.awaitL[L].flatMap { v ⇒
      if (p(v)) dropWhileL(p)(rest)
      else tee.feed1L(v)(rest)
    }

  /*
   * A Tee that drop from right while the predicate `p` is true for the
   * values, then continue with the Tee `rest`
   *
   **/
  def dropWhileR[L, R, O](p: R ⇒ Boolean)(rest: Tee[L, R, O]): Tee[L, R, O] =
    Process.awaitR[R].flatMap { v ⇒
      if (p(v)) dropWhileR(p)(rest)
      else tee.feed1R(v)(rest)
    }

  /**
    * Could be used for cassandra's tables join for examples
    */
  def sortedJoin[L, R, T](keyL: L ⇒ T, keyR: R ⇒ T)(implicit o: Ordering[T]) = {
    def joinTee: Tee[L, R, (L, R)] = Process.awaitL[L].flatMap { l ⇒
      Process.awaitR[R].flatMap { r ⇒
        val lk = keyL(l)
        val rk = keyR(r)
        o.compare(lk, rk) match {
          case 0  ⇒ Process.emit((l, r)) ++ joinTee
          case -1 ⇒ dropWhileL((o.lt(_: T, rk)).compose(keyL))(tee.feed1R(r)(joinTee))
          case 1  ⇒ dropWhileR((o.lt(_: T, lk)).compose(keyR))(tee.feed1L(l)(joinTee))
        }
      }
    }

    joinTee
  }
  //result is List((2 -> "2"), (5 -> "5"), (8 ->"8"))
  //(scalaz.stream.Process(1, 2, 3, 4, 5, 6, 7, 8, 9) tee scalaz.stream.Process("2", "5", "8"))(ScalazRecipes.sortedLoin(identity, _.toInt)).toStream.toList

  def unfoldN(streams: List[Iterator[Int]]) = {
    import scalaz._, Scalaz._
    val procs: List[Process[Task, Int]] = streams.map { iter ⇒
      Process.unfold(iter) { it ⇒
        val next = it.next
        println(Thread.currentThread().getName + ":" + next)
        it.hasNext.option(next → it)
      }
    }

    val pool = java.util.concurrent.Executors.newFixedThreadPool(3)
    merge.mergeN(3)(Process.emitAll(procs))(Strategy.Executor(pool))
  }

  //recipes.ScalazRecipes.unfoldN(List(Iterator.range(1, 10), Iterator.range(1, 20), Iterator.range(1, 30))).runLog.unsafePerformSync

  sealed trait GrowingState {
    def count: Int
  }
  case class State(count: Int = 0) extends GrowingState

  //Transducers

  def distinct[T]: Process1[T, T] = {
    def go(seen: Set[T]): Process1[T, T] =
      Process.await1[T].flatMap { v ⇒
        if (seen(v)) go(seen)
        else Process.emit(v) ++ go(seen + v)
      }
    go(Set.empty[T])
  }

  def monotonic[T <: GrowingState]: Process1[T, T] = {
    def go(count: Long): Process1[T, T] =
      Process.await1[T].flatMap { state ⇒
        if (state.count > count) Process.emit(state) ++ go(state.count)
        else go(count)
      }
    go(0)
  }

  def balanced: Process1[Char, Boolean] = {
    def init: Process1[Char, Boolean] =
      Process.receive1[Char, Boolean] {
        case '{' ⇒ close ++ init
        case _   ⇒ Process.emit(false)
      }

    def close: Process1[Char, Boolean] =
      Process.receive1[Char, Boolean] {
        case '}' ⇒ Process.emit(true)
        case '{' ⇒
          close.flatMap {
            case true  ⇒ close
            case false ⇒ Process.emit(false)
          }
        case _ ⇒ Process.emit(false)
      }

    init
  }

  //recipes.ScalazRecipes.runMonotonic
  def runMonotonic =
    (Process.unfold(Iterator(1, 1, 2, 1, 3, 2, 5, 3, 10)) { it ⇒
      val next = it.next
      if (it.hasNext) Option(State(next) → it) else None
    } pipe monotonic) to sink.lift[Task, GrowingState](state ⇒ Task.delay(println(state)))

  //recipes.ScalazRecipes.runBalanced
  def runBalanced =
    (Process.emitAll(Seq('{', '{', '{', '}', '}', '}')) pipe balanced) to sink
      .lift[Task, Boolean](state ⇒ Task.delay(println(state)))

  /*import com.ambiata.origami._, Origami._
  import com.ambiata.origami.stream.FoldableProcessM._
  import com.ambiata.origami.FoldM
  import com.ambiata.origami._, Origami._
  import com.ambiata.origami.effect.SafeT.SafeTTask
  import scalaz.std.AllInstances._

  def max: FoldM[SafeTTask, Int, Option[Int]] =
    com.ambiata.origami.FoldId.maximum[Int].into[SafeTTask]

  def min: FoldM[SafeTTask, Int, Option[Int]] =
    com.ambiata.origami.FoldId.minimum[Int].into[SafeTTask]

  def Count: FoldM[SafeTTask, Int, Int] =
    com.ambiata.origami.FoldId.count[Int].into[SafeTTask]

  def Plus: FoldM[SafeTTask, Int, Int] =
    com.ambiata.origami.FoldId.plus[Int].into[SafeTTask]

  def CountAndSum: FoldM[SafeTTask, Int, (Int, Int)] =
    Count <*> Plus

  def Mean: FoldM[SafeTTask, Int, Double] = CountAndSum.map {
    case (n, s) =>
      if (n == 0) 0.0 else s.toDouble / n
  }

  //http://origami.readthedocs.io/en/latest/combinators/
  def mean2 = com.ambiata.origami.FoldId.mean[Double]

  def both: FoldM[SafeTTask, Int, (Option[Int], Option[Int])] = max <*> min

  //recipes.ScalazRecipes.folds
  def folds =
    (both run rnd.take(10)).attemptRun.unsafePerformSync

  //recipes.ScalazRecipes.meanR
  def meanR = (Mean run rnd.take(10)).attemptRun.unsafePerformSync
   */

  /**
    *
    *
    */
  def mergeSorted[T: scala.math.Ordering](left: List[T], right: List[T])(
    implicit
    ord: scala.math.Ordering[T]
  ): List[T] = {
    val source0 = emitAll(left)
    val source1 = emitAll(right)

    def choice(l: T, r: T): Tee[T, T, T] =
      if (ord.lt(l, r)) emit(l) ++ nextL(r) else emit(r) ++ nextR(l)

    def nextR(l: T): Tee[T, T, T] =
      tee.receiveROr[T, T, T](emit(l) ++ tee.passL)(r ⇒ choice(l, r))

    def nextL(r: T): Tee[T, T, T] =
      tee.receiveLOr[T, T, T](emit(r) ++ tee.passR)(l ⇒ choice(l, r))

    def init: Tee[T, T, T] =
      tee.receiveLOr[T, T, T](tee.passR)(nextR)

    (source0 tee source1)(init).toSource.runLog.run.toList
  }

  //mergeSorted(List(1,3,5,7), List(2,4,6,8,10))

  /**
    * Json streaming
    */
  def scenario08: Process[Task, Unit] = {
    import jawnstreamz._
    implicit val facade    = jawn.support.spray.Parser.facade
    implicit val scheduler = scalaz.stream.DefaultScheduler

    val chunkSizes: Process[Task, Int] = Process.repeatEval(Task.delay {
      val n = ThreadLocalRandom.current().nextInt(20, 50)
      println(s"read chunk of bytes: $n")
      n
    })

    val jsonSource = (chunkSizes through io.chunkR(new FileInputStream("array.json")))
    val laggedSource: Process[Task, ByteVector] =
      (jsonSource zipWith time.awakeEvery(Random.nextInt(1000).millis))((chunk, _) ⇒ chunk)
    (laggedSource.unwrapJsonArray.map(_.prettyPrint) to io.stdOutLines)
  }
}

//More to read
//https://developer.atlassian.com/blog/2016/03/programming-with-algebra/
//https://partialflow.wordpress.com/2016/04/28/scalaz-streams-parallel-processing-and-better-testing/
