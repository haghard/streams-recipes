package recipes.fs

import java.util.concurrent.{ Executors, ThreadLocalRandom }

import cats.effect.{ ExitCode, IO, IOApp }
import cats.effect._
import cats.implicits._
import fs2._
import fs2.concurrent.Queue
import recipes.{ GraphiteMetrics, GraphiteSupport, TimeWindows }

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

/*
  Flow description:
  The source and sink run at the same rate at the beginning.
  The sink slows down over time, increasing latency on every message.
  I use the bounded queue between the source and the sink.
  This leads to blocking "enqueue" operation for the source in case no space in the queue.
  Result: The source's rate is going to decrease proportionally to the sink's rate.
*/
object scenario_1 extends IOApp with TimeWindows with GraphiteSupport {

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5, FsDaemons("scenario01")))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  def source(latency: FiniteDuration, timeWindow: Long, msg: String, monitoring: GraphiteMetrics,
             q: Queue[IO, Long]): Stream[IO, Unit] =
    Stream.fixedRate[IO](latency)
      .scan(State(item = 0l))((acc, _) ⇒ tumblingWindow(acc, timeWindow))
      .through(graphitePipe(monitoring, msg, { s ⇒ s.item }))
      //.evalMap(state ⇒ send(monitoring, msg).map(_ ⇒ state.item))
      .through(q.enqueue)

  def graphiteSink[A](monitoring: GraphiteMetrics, message: String): Pipe[IO, A, Unit] =
    _.evalMap(_ ⇒ send(monitoring, message))

  def graphitePipe[A, B](monitoring: GraphiteMetrics, message: String, f: A ⇒ B): Pipe[IO, A, B] =
    _.evalMap(i ⇒ send(monitoring, message).map(_ ⇒ f(i)))

  def testSink(i: Long) = IO {
    println(s"${Thread.currentThread.getName}: $i start")
    Thread.sleep(ThreadLocalRandom.current.nextInt(300, 500))
    println(s"${Thread.currentThread.getName}: $i stop")
    i
  }

  def flow: Stream[IO, Unit] = {
    val sName = "scenario1"
    val delayPerMsg = 1l
    val window = 5000l
    val sourceDelay = 5.millis

    val srcMessage = "fs2_src_01:1|c"
    val sinkMessage = "fs2_sink_01:1|c"
    val gSrc = graphiteInstance
    val gSink = graphiteInstance

    (for {
      q ← Stream.eval(Queue.bounded[IO, Long](1 << 6))

      src = Stream.fixedRate[IO](sourceDelay)
        .scan(State(item = 0l))((acc, _) ⇒ tumblingWindow(acc, window))
        .through(graphitePipe(gSrc, srcMessage, { s ⇒ s.item }))
        //.evalMap(state ⇒ send(monitoring, msg).map(_ ⇒ state.item))
        .through(q.enqueue)

      sink = q.dequeue
        .scan(0l)((acc, _) ⇒ slowDown(acc, delayPerMsg))
        .through(graphiteSink[Long](gSink, sinkMessage))
      //.evalMap(_ ⇒ send(monitoring, sinkMessage))

      out ← src mergeHaltL sink
    } yield out)
      .handleErrorWith(th ⇒ Stream.eval(IO(println(s"★ ★ ★  $sName error: ${th.getMessage}  ★ ★ ★"))))
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★  $sName completed ★ ★ ★"))))
  }

  def flow2 = {
    val window = 5000l
    val sourceDelay = 100.millis
    val par = 4

    val src = Stream.fixedRate[IO](sourceDelay)
      .scan(State[Long](item = 0l))((acc, _) ⇒ tumblingWindow(acc, window))
      .map(_.item)
      .take(200)

    //src.balanceN(par, 1 << 5)(testSink(_))

    src.balanceN2(par, 1 << 5)(i ⇒ IO {
      val ind = i % par
      println(s"${Thread.currentThread.getName}: worker:${ind} starts: $i")
      Thread.sleep(ThreadLocalRandom.current.nextInt(100, 500))
      println(s"${Thread.currentThread.getName}: worker:${ind} stop: $i")
      i
    })
  }

  //src.balanceN[Unit](1 << 5, 4)(mapAsyncUnordered[IO, Long, Unit](4)(testSink(_)))
  def mapAsyncUnordered[F[_], A, B](parallelism: Int)(f: A ⇒ F[B])(implicit F: Concurrent[F]): Pipe[F, A, B] =
    (inner: fs2.Stream[F, A]) ⇒
      inner.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism)

  override def run(args: List[String]): IO[ExitCode] =
    flow.compile.drain.as(ExitCode.Success)
    //flow2.compile.drain.as(ExitCode.Success)
}