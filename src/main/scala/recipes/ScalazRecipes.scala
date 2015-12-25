package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ForkJoinPool, ThreadFactory }

import scalaz.{ -\/, \/-, \/ }
import scalaz.stream._
import scalaz.stream.merge._
import scalaz.concurrent.{ Strategy, Task }
import scala.concurrent.duration._

//runMain recipes.ScalazRecipes
object ScalazRecipes extends App {
  val showLimit = 1000
  val observePeriod = 5000
  val limit = Int.MaxValue
  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.47"), 8125)
  val Ex = Strategy.Executor(new ForkJoinPool(Runtime.getRuntime.availableProcessors()))

  case class StreamThreadFactory(name: String) extends ThreadFactory {
    private def namePrefix = s"$name-thread"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup
    override def newThread(r: Runnable) = new Thread(this.group, r,
      s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
  }

  def statsDInstance = new StatsD { override val address = statsD }

  def sleep(latency: Long) = Process.repeatEval(Task.delay(Thread.sleep(latency)))

  def signal = async.signalOf(0)(Strategy.Executor(Executors.newFixedThreadPool(2, new StreamThreadFactory("signal"))))

  scenario02.run[Task].run

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

  def statsDin(statsD: StatsD, message: String) = sink.lift[Task, Int] { _ ⇒
    Task.delay(statsD send message)
  }

  def statsDOut0(s: scalaz.stream.async.mutable.Signal[Int], statsD: StatsD, message: String) = sink.lift[Task, Int] { x: Int ⇒
    s.set(x).map(_ ⇒ statsD send message)
  }

  def statsDOut(s: scalaz.stream.async.mutable.Signal[Int], statsD: StatsD, message: String) = sink.lift[Task, (Long, Int)] { x: (Long, Int) ⇒
    val latency = 0 + (x._1 / 1000)
    Thread.sleep(latency, x._1 % 1000 toInt)
    s.set(x._2).map(_ ⇒ statsD send message)
  }

  def broadcastN[T](n: Int, source: Process[Task, T], bufferSize: Int = 4)(implicit S: Strategy): Process[Task, Seq[Process[Task, T]]] = {
    val queues = (0 until n).map(_ ⇒ async.boundedQueue[T](bufferSize)(S))
    val broadcast = queues./:(source)((src, q) ⇒ (src observe q.enqueue)).onComplete(Process.eval(Task.gatherUnordered(queues.map(_.close))))
    (broadcast.drain merge Process.emit(queues.map(_.dequeue)))(S)
  }

  def broadcastN2[T](n: Int, src: Process[Task, T], waterMark: Int, bufferSize: Int = 4)(implicit S: Strategy): Process[Task, Seq[Process[Task, T]]] = {
    val queues = (0 until n).map(_ ⇒ async.boundedQueue[T](bufferSize)(S))
    val broadcast = queues./:(src) { (s, q) ⇒
      ((q.size.discrete.filter(_ > waterMark) zip q.dequeue).drain merge s.observe(q.enqueue))
    }.onComplete(Process.eval(Task.gatherUnordered(queues.map(_.close))))
    (broadcast.drain merge Process.emit(queues.map(_.dequeue)))(S)
  }

  def interleaveN[T](q: scalaz.stream.async.mutable.Queue[T], ps: List[Process[Task, T]])(implicit S: Strategy): Process[Task, T] = {
    val merge = ps.tail./:(ps.head to q.enqueue) { (acc, c) ⇒ (acc merge (c to q.enqueue))(S) }.onComplete(Process.eval(q.close))
    (merge.drain merge q.dequeue)(S)
  }

  implicit class ProcessOps[T](val p: Process[Task, T]) extends AnyVal {
    import scalaz.stream.ReceiveY.{ HaltOne, ReceiveL, ReceiveR }

    /**
     * Count window
     */
    def throughAllWindow(aggregateInterval: Duration)(implicit S: scalaz.concurrent.Strategy): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis) wye p)(tumblingWye[T](aggregateInterval, false))(S)

    /**
     * Tumbling windows discretize a stream into non-overlapping windows
     */
    def throughTumblingWindow(aggregateInterval: Duration)(implicit S: scalaz.concurrent.Strategy): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis) wye p)(tumblingWye[T](aggregateInterval))(S)

    /**
     * Sliding windows discretize a stream into overlapping windows
     */
    def throughSlidingWindow(aggregateInterval: Duration, numOfUnits: Int)(implicit S: scalaz.concurrent.Strategy): Process[Task, T] =
      (discreteStep(aggregateInterval.toMillis / numOfUnits) wye p)(tumblingWye[T](aggregateInterval))(S)

    private def tumblingWye[I](duration: Duration, reset: Boolean = true): scalaz.stream.Wye[Long, I, I] = {
      val timeWindow = duration.toNanos
      val P = scalaz.stream.Process

      def go(acc: Long, last: Long, n: Int): Wye[Long, I, I] =
        P.awaitBoth[Long, I].flatMap {
          case ReceiveL(currentNanos) ⇒
            if (currentNanos - last > timeWindow) {
              println(buildProgress(n, acc, (currentNanos - last) / 1000000000))
              go(if (reset) 0l else acc + 1l, currentNanos, 1)
            } else {
              println(buildProgress(n, acc, (currentNanos - last) / 1000000000))
              go(acc, last, n + 1)
            }
          case ReceiveR(i) ⇒ P.emit(i) ++ go(acc + 1l, last, n)
          case HaltOne(e)  ⇒ P.Halt(e)
        }

      go(0l, System.nanoTime, 1)
    }

    private def buildProgress(i: Int, acc: Long, sec: Long) =
      s"${List.fill(i)(" ★ ").mkString} number:$acc interval: $sec sec"

    private def discreteStep(millis: Long) =
      Process.repeatEval(Task.delay { Thread.sleep(millis); System.nanoTime })
  }

  def scenario01: Process[Task, Unit] = {
    val s = signal
    val sourceDelay = 20
    val srcMessage = "scalaz-source1:1|c"
    val sinkMessage = "scalaz-sink1:1|c"

    (s.continuous zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario01: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    naturalsEvery(sourceDelay) observe statsDin(statsDInstance, srcMessage) to statsDOut0(s, statsDInstance, sinkMessage)
  }

  /**
   * Situation: A source and a sink perform on the same rates.
   * Result: The source and the sink are going on the same rate.
   */
  def scenario01_1: Process[Task, Unit] = {
    val sourceDelay = 20l
    val latency: Duration = 5 seconds
    val srcMessage = "scalaz-source1:1|c"
    val sinkMessage = "scalaz-sink1:1|c"

    val src = naturalsEvery(sourceDelay).observe(statsDin(statsDInstance, srcMessage))
    (src throughTumblingWindow latency)(Ex) to statsDin(statsDInstance, sinkMessage)
  }

  /**
   * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
   * We are using boundedQueue as buffer between them, which blocks the source in case no space in queue.
   * Result: The source's rate is going to decrease proportionally with the sink's rate.
   */
  def scenario02: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10
    val triggerInterval: Duration = 5 seconds
    val srcMessage = "scalaz-source2:1|c"
    val sinkMessage = "scalaz-sink2:1|c"
    val queue = async.boundedQueue[Int](bufferSize)(Ex)

    (naturalsEvery(sourceDelay) observe queue.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run.runAsync(_ ⇒ ())

    (queue.dequeue.stateScan(0l)({ v: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        updated = latency + delayPerMsg
        _ = Thread.sleep(0 + (updated / 1000), updated % 1000 toInt)
        _ ← scalaz.State.put(updated)
      } yield v
    }) throughSlidingWindow (triggerInterval, 5))(Ex) to statsDin(statsDInstance, sinkMessage)
  }

  /**
   * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
   * We are using circular buffer as buffer between them, it will override elements if no space in buffer.
   * Result: The sink's rate is going to be decrease but the source's rate will be stayed on the initial level.
   */
  def scenario03: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 20
    val window: Duration = 5 seconds
    val cBuffer = async.circularBuffer[Int](bufferSize)(Ex)

    val srcMessage = "scalaz-source3:1|c"
    val sinkMessage = "scalaz-sink3:1|c"

    (naturalsEvery(sourceDelay) observe cBuffer.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(cBuffer.close))
      .run.runAsync(_ ⇒ ())

    (cBuffer.dequeue.stateScan(0l) { n: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        updated = latency + delayPerMsg
        _ = Thread.sleep(0 + (updated / 1000), updated % 1000 toInt)
        _ ← scalaz.State.put(updated)
      } yield n
    } throughTumblingWindow window)(Ex) to statsDin(statsDInstance, sinkMessage)
  }

  /**
   * It behaves like scenario03. The only difference being that we are using queue instead of circular buffer
   */
  def scenario03_1: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val waterMark = bufferSize - 5
    val sourceDelay = 10
    val window: Duration = 5 seconds
    val queue = async.boundedQueue[Int](bufferSize)(Ex)

    val srcMessage = "scalaz-source3_1:1|c"
    val sinkMessage = "scalaz-sink3_1:1|c"

    def dropLastCleaner = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeue).drain

    (naturalsEvery(sourceDelay) observe queue.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run.runAsync(_ ⇒ ())

    val qSink = (queue.dequeue.stateScan(0l) { number: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        updated = latency + delayPerMsg
        _ = Thread.sleep(0 + (updated / 1000), updated % 1000 toInt)
        _ ← scalaz.State.put(updated)
      } yield number
    } throughTumblingWindow window)(Ex) to statsDin(statsDInstance, sinkMessage)

    mergeN(Process(qSink, dropLastCleaner))(Ex)
  }

  /**
   * Usage:
   * val src: Process[Task, Char] = Process.emitAll(Seq('a', 'b', 'c', 'd', 'e', 'g'))
   * (src |> count).runLog.run
   * Vector(\/-(a), -\/(1), \/-(b), -\/(2), \/-(c), -\/(3), \/-(d), -\/(4), \/-(e), -\/(5), \/-(g), -\/(6))
   *
   */
  def count[A]: Process1[A, Long \/ A] = {
    def go(acc: Long): Process1[A, Long \/ A] = {
      Process.receive1[A, Long \/ A] { element: A ⇒
        Process.emitAll(Seq(\/-(element), -\/(acc + 1))) ++ go(acc + 1)
      }
    }
    go(0L)
  }

  /**
   * It's different from scenario03_1 only in dropping the whole BUFFER
   * Fast Source, fast consumer in the beginning get slower
   * Source publish data into queue.
   * We have dropLastStrategy process that tracks queue size and drop ALL BUFFER once we exceed waterMark
   * Consumer, which gets slower (starts at no delay, increase delay with every message.
   * Result: Source stays at the same rate, consumer starts receive partial data
   */
  def scenario03_2: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val waterMark = bufferSize - 5
    val producerRate = 10
    val queue = async.boundedQueue[Int](bufferSize)(Ex)

    val s = signal
    val srcMessage = "scalaz-source3_2:1|c"
    val sinkMessage = "scalaz-sink3_2:1|c"

    def dropBufferProcess = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeueBatch(waterMark)).drain

    ((naturals zip sleep(producerRate)).map(_._1) observe queue.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run[Task].runAsync(_ ⇒ ())

    (s.continuous zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario03_2: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    val qSink = queue.dequeue.stateScan(0l) { number: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ ← scalaz.State.put(increased)
      } yield (increased, number)
    } to statsDOut(s, statsDInstance, sinkMessage)

    mergeN(Process(qSink, dropBufferProcess))(Ex)
  }

  /**
   * Situation:
   *
   */
  def scenario04: Process[Task, Unit] = {
    val delayPerMsg = 2l
    val s0 = signal
    val s1 = signal
    val producerRate = 10
    val srcMessage = "scalaz-source4:1|c"
    val sinkMessage0 = "scalaz-sink4_0:1|c"
    val sinkMessage1 = "scalaz-sink4_1:1|c"

    val src = (naturals zip sleep(producerRate)).map(_._1) observe statsDin(statsDInstance, srcMessage)

    (s1.continuous zip sleep(observePeriod)) //or s0
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario04: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    (for {
      outlets ← broadcastN(2, src)(Ex)

      out0 = outlets(0).stateScan(0l) { number: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          increased = latency + delayPerMsg
          _ ← scalaz.State.put(increased)
        } yield (increased, number)
      } to statsDOut(s0, statsDInstance, sinkMessage0)

      out1 = outlets(1) to statsDOut0(s1, statsDInstance, sinkMessage1)
      _ ← (out0 merge out1)(Ex)
    } yield ())
  }

  /**
   * Situation:
   *
   */
  def scenario05: Process[Task, Unit] = {
    val delayPerMsg = 2l
    val s0 = signal
    val s1 = signal
    val producerRate = 10
    val bufferSize = 1 << 7
    val waterMark = bufferSize - 5
    val srcMessage = "scalaz-source5:1|c"
    val sinkMessage0 = "scalaz-sink5_0:1|c"
    val sinkMessage1 = "scalaz-sink5_1:1|c"
    val src = (naturals zip sleep(producerRate)).map(_._1) observe statsDin(statsDInstance, srcMessage)

    (s1.continuous zip sleep(observePeriod)) //or s0
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario05: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    (for {
      outlets ← broadcastN2(2, src, waterMark, bufferSize)(Ex)

      out0 = outlets(0).stateScan(0l) { number: Int ⇒
        for {
          latency ← scalaz.State.get[Long]
          increased = latency + delayPerMsg
          _ ← scalaz.State.put(increased)
        } yield (increased, number)
      } to statsDOut(s0, statsDInstance, sinkMessage0)

      out1 = outlets(1) to statsDOut0(s1, statsDInstance, sinkMessage1)
      _ ← (out0 merge out1)(Ex)
    } yield ())
  }

  /**
   * Situation:
   *
   */
  def scenario07: Process[Task, Unit] = {
    val s = signal
    val latencies = List(20l, 30l, 40l, 45l)
    val sources = latencies.zipWithIndex.map { ms ⇒
      (naturals zip sleep(ms._1)).map(_._1) observe statsDin(statsDInstance, s"scalaz-source07_${ms._2}:1|c")
    }

    (s.continuous zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario07: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    interleaveN(async.boundedQueue[Int](2 << 7)(Ex), sources)(Ex) to statsDOut0(s, statsDInstance, "scalaz-sink7:1|c")
  }
}