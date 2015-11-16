package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.ThreadFactory
import java.util.concurrent.Executors._

import scalaz.stream.{ Process, process1, async }
import scalaz.stream.merge._
import scalaz.concurrent.{ Strategy, Task }

//runMain recipes.ScalazRecipes
object ScalazRecipes extends App {

  case class StreamThreadFactory(name: String) extends ThreadFactory {
    private def namePrefix = name + "-thread"
    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup
    override def newThread(r: Runnable) = new Thread(this.group, r,
      s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
  }

  val limit = Int.MaxValue

  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.134"), 8125)

  def statsDInstance = new StatsD { override val address = statsD }

  def sleep(latency: Long) = Process.repeatEval(Task.delay(Thread.sleep(latency)))

  val Pub = newFixedThreadPool(1, StreamThreadFactory("pub"))
  val Sub = Strategy.Executor(newFixedThreadPool(4, StreamThreadFactory("sub")))

  //scenario013_2.runLast.run
  scenario03.runLast.run

  def naturals: Process[Task, Int] = {
    def go(i: Int): Process[Task, Int] =
      Process.await(Task.now(i))(i ⇒ Process.emit(i) ++ go(i + 1))
    go(0)
  }

  /**
   * Fast publisher and fast consumer in the beginning,
   * consumer gets slower, increases delay with every message.
   * We use boundedQueue in between which makes producer slower in case no space in queue (blocking)
   * Result: Publisher and consumer will start at same rate. Publisher's rate will go down together with consumer.
   */
  def scenario02: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10
    val showLimit = 10000
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val statsDSink = statsDInstance
    val srcMessage = "scalaz-source2:1|c"
    val sinkMessage = "scalaz-sink2:1|c"

    Task.fork {
      val statsD = statsDInstance
      ((naturals zip sleep(sourceDelay)) |> process1.lift { x ⇒
        (queue enqueueOne (x._1) run) //backpressure
        (statsD send srcMessage)
      }).onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ ())

    queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
      } yield (increased, number)
    } |> process1.lift { x ⇒
      val latency = 0 + (x._1 / 1000)
      if (x._2 % showLimit == 0)
        println(latency)
      Thread.sleep(latency, x._1 % 1000 toInt)
      (statsDSink send sinkMessage)
    }
  }

  /**
   * Fast publisher and fast consumer in the beginning, consumer gets slower, increase delay with every message.
   * We use circular buffer in between that leads to overriding oldest messages
   * Result: publisher stays at the original rate and starts override oldest messages, consumer is getting slower
   */
  def scenario03: Process[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10
    val showLimit = 10000
    val cBuffer = async.circularBuffer[Int](bufferSize)(Sub)
    val statsDSink = statsDInstance
    val statsDSource = statsDInstance

    val srcMessage = "scalaz-source3:1|c"
    val sinkMessage = "scalaz-sink3:1|c"

    Task.fork {
      ((naturals zip sleep(sourceDelay)) |> process1.lift[(Int, Unit), Unit] { x ⇒
        (cBuffer enqueueOne (x._1) run)
        (statsDSource send srcMessage)
      }).onComplete(Process.eval_(cBuffer.close)).run[Task]
    }(Pub).runAsync(_ ⇒ ())

    cBuffer.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
      } yield (increased, number)
    } |> process1.lift { x ⇒
      val latency = 0 + (x._1 / 1000)
      if (x._2 % showLimit == 0)
        println(latency)
      Thread.sleep(latency, x._1 % 1000 toInt)
      (statsDSink send sinkMessage)
    }
  }

  /**
   * Fast publisher, fast consumer in the beginning later getting slower
   * Producer publish data into queue.
   * We have dropLastProcess process that tracks queue size and drop LAST ELEMENT once we exceed waterMark
   * Result: publisher stays at the same rate, consumer starts receive partial data
   */
  def scenario03_1: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 6
    val waterMark = bufferSize - 3
    val producerRate = 30
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val statsDSink = statsDInstance
    val srcMessage = "scalaz-source3_1:1|c"
    val sinkMessage = "scalaz-sink3_1:1|c"

    def dropLastProcess = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeue).drain

    //Publisher
    Task.fork {
      val statsD = statsDInstance
      ((naturals zip sleep(producerRate)) |> process1.lift[(Int, Unit), Unit] { x ⇒
        (queue enqueueOne (x._1) run)
        (statsD send srcMessage)
      }).onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ ())

    val sink = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
      } yield (increased, number)
    } |> process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (statsDSink send sinkMessage)
    }

    mergeN(Process(sink, dropLastProcess))(Sub)
  }

  /**
   * It's different from scenario03_1 only in dropping the whole BUFFER
   * Fast publisher, fast consumer in the beginning get slower
   * Producer publish data into queue.
   * We have dropLastStrategy process that tracks queue size and drop ALL BUFFER once we exceed waterMark
   * Consumer, which gets slower (starts at no delay, increase delay with every message.
   * Result: publisher stays at the same rate, consumer starts receive partial data
   */
  def scenario03_2: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 6
    val waterMark = bufferSize - 3
    val producerRate = 30
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val statsDSink = statsDInstance
    val srcMessage = "scalaz-source3_2:1|c"
    val sinkMessage = "scalaz-sink3_2:1|c"

    def dropBufferProcess = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeueBatch(waterMark)).drain

    Task.fork {
      val statsDSource = statsDInstance
      ((naturals zip sleep(producerRate)) |> process1.lift[(Int, Unit), Unit] { x ⇒
        (queue enqueueOne (x._1) run)
        (statsDSource send srcMessage)
      }).onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ ())

    val sink = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        increased = latency + delayPerMsg
        _ <- scalaz.State.put(increased)
      } yield (increased, number)
    } |> process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (statsDSink send sinkMessage)
    }

    mergeN(Process(sink, dropBufferProcess))(Sub)
  }
}