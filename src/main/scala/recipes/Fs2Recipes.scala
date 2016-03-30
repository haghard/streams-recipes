package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ Executors, ThreadFactory }

import fs2.async.mutable
import fs2.process1._
import fs2.util.Task
import fs2.{ Stream, async, time }
import scala.concurrent.duration._

import scala.concurrent.duration.Duration

//runMain recipes.Fs2Recipes
object Fs2Recipes extends App {
  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.182"), 8125)

  def grafanaInstance = new Grafana {
    override val address = statsD
  }

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

  implicit val qStrategy = fs2.Strategy.fromExecutor(Executors.newFixedThreadPool(2, RecipesDaemons("queue")))

  implicit val scheduler = java.util.concurrent.Executors.newScheduledThreadPool(2, RecipesDaemons("natur"))

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
  }*/

  def grafana(msg: String): fs2.Process1[Int, Unit] = lift { (acc: Int) ⇒
    println(Thread.currentThread().getName + ": sink " + acc)
  }

  scenario02.runLog.run.run

  def naturalsEvery(sourceDelay: Duration, msg: String, q: mutable.Queue[Task, Int]): Stream[Task, Nothing] =
    time.awakeEvery(sourceDelay)(fs2.Strategy.fromExecutor(scheduler), scheduler)
      .scan(0) { (acc, d) ⇒
        acc + 1
      }.evalMap { d: Int ⇒
        println(Thread.currentThread().getName + "source " + d)
        q.enqueue1(d)
      }.drain

  def introduceDelay(state: (Long, Int), current: Int, delayPerMsg: Long) = {
    val latency = state._1 + delayPerMsg
    Thread.sleep(0 + (latency / 1000), latency % 1000 toInt)
    (latency, current)
  }

  /**
   * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increasing delay with every message.
   * We are using boundedQueue as buffer between them, which leads to blocking the source in case no space in the queue.
   * Result: The source's rate is going to decrease proportionally with the sink's rate.
   */
  def scenario02: Stream[Task, Unit] = {
    val delayPerMsg = 1l
    val bufferSize = 1 << 7
    val sourceDelay = 10.millis

    val srcMessage = "scalaz-source1:1|c"
    val sinkMessage = "scalaz-sink1:1|c"

    val flow = for {
      q ← Stream.eval(async.boundedQueue[Task, Int](bufferSize))
      out ← naturalsEvery(sourceDelay, srcMessage, q) merge q.dequeue.scan((0l, 0))((acc, c) ⇒ introduceDelay(acc, c, delayPerMsg))
    } yield out

    (flow.map(_._2) pipe grafana(sinkMessage))
      .onError[Task, Unit] { ex: Throwable ⇒ fs2.Stream.eval(Task.now(println(ex.getMessage))) }
  }
}