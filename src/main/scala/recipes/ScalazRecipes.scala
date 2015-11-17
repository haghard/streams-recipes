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
  val watchPeriod = 3000
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

  scenario03_2.run[Task].run

  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.now(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }

  def statsDin(statsD: StatsD, message: String) = sink.lift[Task, Int] { _ =>
    Task.delay(statsD send message)
  }

  def statsDOut(s: scalaz.stream.async.mutable.Signal[Int], statsD: StatsD, message: String) = sink.lift[Task, (Long, Int)] { x: (Long, Int) =>
    val latency = 0 + (x._1 / 1000)
    Thread.sleep(latency, x._1 % 1000 toInt)
    s.set(x._2).map(_ => statsD send message)
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
      .run.runAsync(_ => ())

    (Process.repeatEval(s.get) zip sleep(watchPeriod))
      .to(sink.lift[Task, (Int, Unit)] { x => Task.delay(println(s"scalaz-scenario02: ${x._1}")) })
      .run.runAsync(_ => ())

    queue.dequeue.stateScan(0l)({ number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
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
      .run[Task].runAsync(_ => ())

    (Process.repeatEval(s.get) zip sleep(watchPeriod))
      .to(sink.lift[Task, (Int, Unit)] { x => Task.delay(println(s"scalaz-scenario03: ${x._1}")) })
      .run.runAsync(_ => ())

    cBuffer.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
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
      .run[Task].runAsync(_ => ())

    (Process.repeatEval(s.get) zip sleep(watchPeriod))
      .to(sink.lift[Task, (Int, Unit)] { x => Task.delay(println(s"scalaz-scenario03_1: ${x._1}")) })
      .run.runAsync(_ => ())

    val qSink = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
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
      .run[Task].runAsync(_ => ())

    (Process.repeatEval(s.get) zip sleep(watchPeriod))
      .to(sink.lift[Task, (Int, Unit)] { x => Task.delay(println(s"scalaz-scenario03_2: ${x._1}")) })
      .run.runAsync(_ => ())

    val qSink = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
      } yield (increased, number)
    } to statsDOut(s, statsDInstance, sinkMessage)

    mergeN(Process(qSink, dropBufferProcess))(Ex)
  }
}