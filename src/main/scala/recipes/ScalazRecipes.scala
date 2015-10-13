package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.Executors
import java.util.concurrent.Executors._

import scalaz.stream.{ merge, Process, process1, async }
import scalaz.concurrent.{ Strategy, Task }

//runMain recipes.ScalazRecipes
object ScalazRecipes extends App {

  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.134"), 8125)

  def statsDPoint = new StatsD {
    override val address = statsD
  }

  def sleep(latency: Long) = Process.repeatEval(Task.delay(Thread.sleep(latency)))

  val Pub = newSingleThreadExecutor()
  val Sub = Strategy.Executor(Executors.newFixedThreadPool(4))

  scenario023.runLast.run

  /**
   * Fast publisher, fast consumer in the beginning get slower, no buffer
   * - same publisher as step 1. (e.g 50msg/s)
   * - consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher and consumer will start at same rate. Publish rate will go down
   * together with publisher rate.
   *
   */
  def scenario02: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 5
    val producerRate = 20
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val cPoint = statsDPoint
    val consumeP = process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (cPoint send s"Consumer2:1|c")
    }

    val pPoint = statsDPoint
    val publishP = process1.lift[(Int, Unit), Unit] { x ⇒
      (queue enqueueOne (x._1) run)
      (pPoint send s"Publisher2:1|c")
    }

    Task.fork {
      ((Process.emitAll(1 to 10000) zip sleep(producerRate)) |> publishP)
        .onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ println("Publisher2 is done"))

    queue.dequeue |> (process1.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        updatedLatency = latency + delayPerMsg
        _ <- scalaz.State.put(updatedLatency)
      } yield (updatedLatency, number)
    }) |> consumeP
  }

  /**
   * Fast publisher, fast consumer in the beginning get slower
   * - Producer overrides oldest data in circularBuffer without any blocking,
   * - Consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher stays at the same rate, consumer starts receive partial data
   */
  def scenario03: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 5
    val producerRate = 30
    val cBuffer = async.circularBuffer[Int](bufferSize)(Sub)

    val cPoint = statsDPoint
    val consumeP = process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (cPoint send s"Consumer3:1|c")
    }

    val pPoint = statsDPoint
    val publishP = process1.lift[(Int, Unit), Unit] { x ⇒
      (cBuffer enqueueOne (x._1) run)
      (pPoint send s"Publisher3:1|c")
    }

    Task.fork {
      ((Process.emitAll(1 to 10000) zip sleep(producerRate)) |> publishP)
        .onComplete(Process.eval_(cBuffer.close)).run[Task]
    }(Pub).runAsync(_ ⇒ println("Publisher3 is done"))

    cBuffer.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        updatedLatency = latency + delayPerMsg
        _ <- scalaz.State.put(updatedLatency)
      } yield (updatedLatency, number)
    } |> consumeP
  }

  /**
   * Fast publisher, fast consumer in the beginning get slower
   * - Producer publish data into queue.
   * We have dropLastStrategy process that tracks queue size and drop last element once we exceed waterMark
   * - Consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher stays at the same rate, consumer starts receive partial data
   */
  def scenario013: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 6
    val waterMark = bufferSize - 3
    val producerRate = 30
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val cPoint = statsDPoint
    val pPoint = statsDPoint

    val dropLastStrategy = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeue).drain

    val consumer = process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (cPoint send s"Consumer3_1:1|c")
    }

    val publisher = process1.lift[(Int, Unit), Unit] { x ⇒
      (queue enqueueOne (x._1) run)
      (pPoint send s"Publisher3_1:1|c")
    }

    Task.fork {
      ((Process.emitAll(1 to 10000) zip sleep(producerRate)) |> publisher)
        .onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ println("Publisher3_2 has done"))

    val flow = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        updatedLatency = latency + delayPerMsg
        _ <- scalaz.State.put(updatedLatency)
      } yield (updatedLatency, number)
    } |> consumer

    merge.mergeN(Process(flow, dropLastStrategy))(Sub)
  }

  /**
   * Fast publisher, fast consumer in the beginning get slower
   * - Producer publish data into queue.
   * We have dropLastStrategy process that tracks queue size and drop all buffer once we exceed waterMark
   * - Consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher stays at the same rate, consumer starts receive partial data
   */
  def scenario023: Process[Task, Unit] = {
    val initDelay = 0
    val delayPerMsg = 20l
    val bufferSize = 1 << 6
    val waterMark = bufferSize - 3
    val producerRate = 30
    val queue = async.boundedQueue[Int](bufferSize)(Sub)

    val cPoint = statsDPoint
    val pPoint = statsDPoint

    val dropBufferStrategy = (queue.size.discrete.filter(_ > waterMark) zip queue.dequeueBatch(waterMark)).drain

    val consumer = process1.lift[(Long, Int), Unit] { x ⇒
      Thread.sleep(initDelay + (x._1 / 1000), x._1 % 1000 toInt)
      (cPoint send s"Consumer3_2:1|c")
    }

    val publisher = process1.lift[(Int, Unit), Unit] { x ⇒
      (queue enqueueOne (x._1) run)
      (pPoint send s"Publisher3_2:1|c")
    }

    Task.fork {
      ((Process.emitAll(1 to 10000) zip sleep(producerRate)) |> publisher)
        .onComplete(Process.eval_(queue.close)).run[Task]
    }(Pub).runAsync(_ ⇒ println("Publisher3_2 has done"))

    val flow = queue.dequeue.stateScan(0l) { number: Int =>
      for {
        latency <- scalaz.State.get[Long]
        updatedLatency = latency + delayPerMsg
        _ <- scalaz.State.put(updatedLatency)
      } yield (updatedLatency, number)
    } |> consumer

    merge.mergeN(Process(flow, dropBufferStrategy))(Sub)
  }
}