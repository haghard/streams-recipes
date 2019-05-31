package recipes.fs

import java.util.concurrent.{ Executors, ThreadLocalRandom }

import cats.effect.{ ExitCode, IO, IOApp }
import cats.effect._
import cats.implicits._
import fs2._
import fs2.concurrent.{ Queue, Signal, SignallingRef }
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

  //  https://fs2.io/concurrency-primitives.html

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5, FsDaemons("scenario01")))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)

  def stop[T](in: Stream[IO, T]): Stream[IO, T] = {
    val signal: Stream[IO, Boolean] = Signal.constant[IO, Boolean](false).discrete
    in.interruptWhen(signal)
  }

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

  def ts[T](i: T) = IO {
    println(s"${Thread.currentThread.getName}: $i start")
    Thread.sleep(ThreadLocalRandom.current.nextInt(300, 500))
    println(s"${Thread.currentThread.getName}: $i stop")
    i
  }

  def worker[T](i: Int): Long ⇒ IO[Unit] =
    (out: Long) ⇒ IO[Unit] {
      println(s"${Thread.currentThread.getName}: w:$i start $out")
      Thread.sleep(ThreadLocalRandom.current().nextLong(100l, 300l))
      println(s"${Thread.currentThread.getName}: w:$i stop $out")
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
    val sourceDelay = 50.millis
    val par = 4

    val src = Stream.fixedRate[IO](sourceDelay)
      .scan(State[Long](item = 0l))((acc, _) ⇒ tumblingWindow(acc, window))
      .map(_.item)
      .take(200)

    //src.balanceN(par, 1 << 5)(testSink(_))

    /*src.balanceN2(par, 1 << 5)(i ⇒ IO {
      val ind = i % par
      println(s"${Thread.currentThread.getName}: worker:${ind} starts: $i")
      Thread.sleep(ThreadLocalRandom.current.nextInt(100, 500))
      println(s"${Thread.currentThread.getName}: worker:${ind} stop: $i")
      i
    })*/

    src.balanceN3(par, 1 << 4) { ind: Int ⇒ (e) ⇒ IO {
      println(s"${Thread.currentThread.getName}: w:$ind starts: $e")
      Thread.sleep(ThreadLocalRandom.current.nextInt(100, 500))
      println(s"${Thread.currentThread.getName}: w:$ind stop: $e")
      e
    }
    }

  }

  //static number of workers
  def oneToManyFlow =
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Option[Long]](1 << 3))
      src = Stream
        .fixedRate[IO](50.millis)
        .scan[Option[Long]](Some(0l))((a, _) ⇒ a.map(_ + 1l))
        .take(200)
        .through(q.enqueue)
        .onComplete(Stream.fixedRate[IO](500.millis).map(_ ⇒ None).through(q.enqueue))

      all = Seq(
        q.dequeue.unNoneTerminate.evalMap(worker(0)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(1)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(2)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(3)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(4)(_)))

      sinks = all.reduce(_ merge _) //wait for all

      //wait for either which in our  case should be  the sinks, because src  never terminates
      flow ← sinks mergeHaltBoth src.drain
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★  OneToMany completed ★ ★ ★"))))

  sealed trait Event

  case class Task(id: Long) extends Event

  case object PoisonPill extends Event

  def sub[T](num: Int, s: SignallingRef[IO, Int]): Event ⇒ IO[Unit] =
    (in: Event) ⇒
      in match {
        case Task(id) ⇒
          IO {
            println(s"${Thread.currentThread.getName}: w:$num start $id")
            Thread.sleep(ThreadLocalRandom.current.nextLong(100l, 300l))
            println(s"${Thread.currentThread.getName}: w:$num stop $id")
          }
        case PoisonPill ⇒
          IO {
            println(s"${Thread.currentThread.getName}: got Quit $num")
            Thread.sleep(ThreadLocalRandom.current.nextLong(100l, 300l))
          } *> s.update(_ - 1).flatMap(_ ⇒ s.get.map(println(_)))
      }

  //Shouldn't lose messages here, as we  bufferSize == countdown(counter) + (delay == 200.millis) which is average latency
  def flow3 = {
    val bufferSize = 1 << 4
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Event](bufferSize))
      interrupter ← Stream.eval(SignallingRef[IO, Int](bufferSize))

      src = Stream
        .fixedRate[IO](50.millis)
        .scan[Task](Task(0l))((a, _) ⇒ a.copy(a.id + 1l))
        .take(200)
        .through(q.enqueue)
        .onComplete(Stream.fixedRate[IO](250.millis).map(_ ⇒ PoisonPill).through(q.enqueue))
        .interruptWhen(interrupter.discrete.map(_ <= 0))

      sinks = q.dequeue.evalMap(sub(0, interrupter)(_)) merge q.dequeue.evalMap(sub(1, interrupter)(_))

      flow ← sinks mergeHaltBoth src
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★ Interrupter flow completed ★ ★ ★"))))
  }

  //we lose messages when stop  the sink
  def flow4 = {
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Long](1 << 4))

      interrupter ← Stream.eval(SignallingRef[IO, Boolean](false))

      src = Stream
        .fixedRate[IO](50.millis)
        .scan[Long](0l)((a, _) ⇒ a + 1l)
        .take(200)
        .through(q.enqueue)
        .onComplete(Stream.eval(interrupter.set(true)))

      sinks = (q.dequeue.evalMap(worker(0)(_)) merge q.dequeue.evalMap(worker(1)(_)))
        .interruptWhen(interrupter)

      flow ← sinks mergeHaltBoth src
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★ Interrupter flow completed ★ ★ ★"))))
  }

  //src.balanceN[Unit](1 << 5, 4)(mapAsyncUnordered[IO, Long, Unit](4)(testSink(_)))
  def mapAsyncUnordered[F[_], A, B](parallelism: Int)(f: A ⇒ F[B])(implicit F: Concurrent[F]): Pipe[F, A, B] =
    (inner: fs2.Stream[F, A]) ⇒
      inner.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism)

  override def run(args: List[String]): IO[ExitCode] =
    //flow3.compile.drain.as(ExitCode.Success)
    //flow4.compile.drain.as(ExitCode.Success)
    //oneToManyFlow.compile.drain.as(ExitCode.Success)
    flow2.compile.drain.as(ExitCode.Success)
}