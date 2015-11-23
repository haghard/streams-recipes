package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ForkJoinPool, ThreadFactory }

import scalaz.stream.{ sink, Process, async }
import scalaz.stream.merge._
import scalaz.concurrent.{ Strategy, Task }

//runMain recipes.ScalazRecipes
object ScalazRecipes extends App {
  val showLimit = 1000
  val observePeriod = 3000
  val limit = Int.MaxValue
  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.134"), 8125)
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

  scenario07.run[Task].run

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

  def mergeP[T](q: scalaz.stream.async.mutable.Queue[T], ps: List[Process[Task, T]])(implicit S: Strategy): Process[Task, T] = {
    val merge = ps.tail./:(ps.head to q.enqueue) { (acc, c) ⇒ (acc merge (c to q.enqueue))(S) }.onComplete(Process.eval(q.close))
    (merge.drain merge q.dequeue)(S)
  }

  def scenario01: Process[Task, Unit] = {
    val s = signal
    val sourceDelay = 10
    val srcMessage = "scalaz-source1:1|c"
    val sinkMessage = "scalaz-sink1:1|c"

    (Process.repeatEval(s.get) zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario01: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    (naturals zip sleep(sourceDelay)).map(_._1) observe statsDin(statsDInstance, srcMessage) to statsDOut0(s, statsDInstance, sinkMessage)
  }

  /**
   * Fast Source and fast consumer in the beginning,
   * consumer gets slower, increases delay with every message.
   * We use boundedQueue in between which makes producer slower in case no space in queue (blocking)
   * Result: Source and consumer will start at same rate. Publisher's rate will go down together with consumer.
   */
  def scenario02: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10
    val srcMessage = "scalaz-source2:1|c"
    val sinkMessage = "scalaz-sink2:1|c"
    val queue = async.boundedQueue[Int](bufferSize)(Ex)
    val s = signal

    ((naturals zip sleep(sourceDelay)).map(_._1) observe queue.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run.runAsync(_ ⇒ ())

    (Process.repeatEval(s.get) zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario02: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    queue.dequeue.stateScan(0l)({ number: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ ← scalaz.State.put(increased)
      } yield (increased, number)
    }) to statsDOut(s, statsDInstance, sinkMessage)
  }

  /**
   * Fast Source and fast consumer in the beginning, consumer gets slower, increases delay with every message.
   * We use circular buffer in between that leads to overriding oldest messages
   * Result: Source stays at the original rate and starts override oldest messages, sink is getting slower
   */
  def scenario03: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10
    val cBuffer = async.circularBuffer[Int](bufferSize)(Ex)

    val s = signal
    val srcMessage = "scalaz-source3:1|c"
    val sinkMessage = "scalaz-sink3:1|c"

    ((naturals zip sleep(sourceDelay)).map(_._1) observe cBuffer.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(cBuffer.close))
      .run[Task].runAsync(_ ⇒ ())

    (Process.repeatEval(s.get) zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario03: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    cBuffer.dequeue.stateScan(0l) { number: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ ← scalaz.State.put(increased)
      } yield (increased, number)
    } to statsDOut(s, statsDInstance, sinkMessage)
  }

  /**
   * Fast Source, fast consumer in the beginning later getting slower
   * Producer publish data into queue.
   * We have dropLastProcess process that tracks queue size and drop LAST ELEMENT once we exceed waterMark
   * Result: Source stays at the same rate, consumer starts receive partial data
   */
  def scenario03_1: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val waterMark = bufferSize - 5
    val producerRate = 10
    val queue = async.boundedQueue[Int](bufferSize)(Ex)

    val s = signal
    val srcMessage = "scalaz-source3_1:1|c"
    val sinkMessage = "scalaz-sink3_1:1|c"

    def dropLastProcess = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeue).drain

    ((naturals zip sleep(producerRate)).map(_._1) observe queue.enqueue to statsDin(statsDInstance, srcMessage))
      .onComplete(Process.eval_(queue.close))
      .run[Task].runAsync(_ ⇒ ())

    (Process.repeatEval(s.get) zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario03_1: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    val qSink = queue.dequeue.stateScan(0l) { number: Int ⇒
      for {
        latency ← scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ ← scalaz.State.put(increased)
      } yield (increased, number)
    } to statsDOut(s, statsDInstance, sinkMessage)

    mergeN(Process(qSink, dropLastProcess))(Ex)
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

    (Process.repeatEval(s.get) zip sleep(observePeriod))
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

  def scenario04: Process[Task, Unit] = {
    val delayPerMsg = 2l
    val s0 = signal
    val s1 = signal
    val producerRate = 10
    val srcMessage = "scalaz-source4:1|c"
    val sinkMessage0 = "scalaz-sink4_0:1|c"
    val sinkMessage1 = "scalaz-sink4_1:1|c"

    val src = (naturals zip sleep(producerRate)).map(_._1) observe statsDin(statsDInstance, srcMessage)

    (Process.repeatEval(s1.get) zip sleep(observePeriod)) //or s0
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

    (Process.repeatEval(s1.get) zip sleep(observePeriod)) //or s0
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

  def scenario07: Process[Task, Unit] = {
    val s = signal
    val latencies = List(20l, 30l, 40l, 45l)
    val ps = latencies.zipWithIndex.map { ms ⇒
      (naturals zip sleep(ms._1)).map(_._1) observe statsDin(statsDInstance, s"scalaz-source07_${ms._2}:1|c")
    }

    (Process.repeatEval(s.get) zip sleep(observePeriod))
      .to(sink.lift[Task, (Int, Unit)] { x ⇒ Task.delay(println(s"scalaz-scenario07: ${x._1}")) })
      .run.runAsync(_ ⇒ ())

    mergeP(async.boundedQueue[Int](2 << 7)(Ex), ps)(Ex) to statsDOut0(s, statsDInstance, "scalaz-sink7:1|c")
  }
}