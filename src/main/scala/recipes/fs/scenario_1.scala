package recipes.fs

import java.util.concurrent.{Executors, ThreadLocalRandom}

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect._
import cats.implicits._
import fs2._
import fs2.concurrent.{Queue, Signal, SignallingRef}
import recipes.{GraphiteMetrics, GraphiteSupport, TimeWindows}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

/*
  The setup of the scenario is as follows:
    The source and sink run at the same rate at the beginning.
    The sink slows down over time, increasing latency on every message.
    I use the bounded queue between the source and the sink.
    This leads to blocking "enqueue" operation for the source in case no space in the queue.
    Result: The source's rate is going to decrease proportionally to the sink's rate.

https://fs2.io/concurrency-primitives.html
https://www.beyondthelines.net/programming/streaming-patterns-with-fs2/

runMain recipes.fs.scenario_1

 */
object scenario_1 extends IOApp with TimeWindows with GraphiteSupport {

  implicit val ec                   = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5, FsDaemons("scenario01")))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val t                    = cats.effect.IO.timer(ec)

  /*
    fs2.Stream.emits(1 to 10000)
      .chunkN(10)
      .covary[IO]
      .parEvalMap(10)(writeToDB[IO])
      .compile
      .drain
      .unsafeRunSync
   */
  def writeToDB[F[_]: Async](chunk: Chunk[Int]): F[Unit] =
    Async[F].async { cb ⇒
      println(s"Writing batch of $chunk to database by ${Thread.currentThread.getName}")
      cb(Right(()))
    }

  def evalAsync[F[_]: Async, T: cats.Monoid](chunk: Chunk[Long]): F[Long] =
    Async[F].async { cb ⇒
      val r = chunk.foldLeft(cats.Monoid[Long].empty)(_ |+| _)
      Thread.sleep(500)
      println(Thread.currentThread.getName + " > " + r)
      cb(Right(r))
    }

  def pipeAsync[F[_]: Sync: LiftIO]: Stream[F, Long] ⇒ Stream[F, Long] =
    _.evalTap { i ⇒
      (IO.shift *> IO.sleep(100.millis) *> IO(i))
        .runAsync({ in ⇒
          in match {
            case Left(e) ⇒
              IO.raiseError(e)
            case Right(r) ⇒
              IO(println(s"processing by ${Thread.currentThread.getName}")) *> IO(r)
          }
        })
        .to[F]
    //.runAsync(_ ⇒ IO.unit).to[F]
    }

  Stream
    .emits(1L to 100L)
    .covary[IO]
    .chunkN(10, true)
    .parEvalMap(4)(evalAsync[IO, Long])
    .compile
    .foldMonoid(cats.Monoid[Long])
    .unsafeRunAsync(r ⇒ println("" + r))

  Stream
    .emits(1L to 100L)
    .covary[IO]
    .chunkN(10, true)
    .parEvalMap(4)(evalAsync[IO, Long])
    .compile
    .drain
    .unsafeRunAsync(_ ⇒ ())

  //fs2.Stream.emits(1l to 100l).covary[IO].through(pipeAsync[IO]).compile.drain.unsafeRunSync

  //fs2.Stream.emits(1l to 100l).through(sumEvery(10)).covary[IO].compile.foldMonoid(cats.Monoid[Long]).unsafeRunSync()
  //fs2.Stream.emits(1l to 100l).through(sumEvery(10)).covary[IO].compile.drain.unsafeRunSync()
  //fs2.Stream.emits(1l to 100l).through(sumEvery(10)).covary[IO].compile.toList.unsafeRunSync()

  def sumEvery[F[_], T: cats.Monoid](batchSize: Int): Pipe[F, T, T] = { in ⇒
    def go(s: Stream[F, T]): Pull[F, T, Unit] =
      s.pull.unconsN(batchSize, true).flatMap {
        case Some((chunk, tailStr)) ⇒
          val chunkResult: T = chunk.foldLeft(cats.Monoid[T].empty)(_ |+| _)
          println(chunkResult)
          //Sync[F].delay(println(chunkResult)) *>
          Pull.output1(chunkResult) >> go(tailStr)
        case None ⇒
          Pull.done
      }
    go(in).stream
  }

  //type Pipe[F[_], -I, +O] = Stream[F, I] => Stream[F, O]
  def pipe2Graphite[F[_]: Sync, T](
    monitoring: GraphiteMetrics,
    message: String
  ): Pipe[F, T, T] /*Stream[F, T] => Stream[F, T]*/ =
    _.evalTap { _ ⇒
      Sync[F].delay(send(monitoring, message))
    //Sync[F].delay(println(s"Stage $name processing $i by ${Thread.currentThread.getName}"))
    }

  def pipe2GraphitePar[F[_]: Sync: LiftIO, T](
    monitoring: GraphiteMetrics,
    message: String
  ): Stream[F, T] ⇒ Stream[F, T] =
    _.evalTap { _ ⇒
      (IO.shift *> IO(send(monitoring, message))).runAsync(_ ⇒ IO.unit).to[F]
    }

  def stop[T](in: Stream[IO, T]): Stream[IO, T] = {
    val signal: Stream[IO, Boolean] = Signal.constant[IO, Boolean](false).discrete
    in.interruptWhen(signal)
  }

  def source(
    latency: FiniteDuration,
    timeWindow: Long,
    msg: String,
    monitoring: GraphiteMetrics,
    q: Queue[IO, Long]
  ): Stream[IO, Unit] =
    Stream
      .fixedRate[IO](latency)
      .scan(State(item = 0L))((acc, _) ⇒ tumblingWindow(acc, timeWindow))
      .through(graphitePipe(monitoring, msg, { s ⇒
        s.item
      }))
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
    (out: Long) ⇒
      IO[Unit] {
        println(s"${Thread.currentThread.getName}: w:$i start $out")
        Thread.sleep(ThreadLocalRandom.current().nextLong(100L, 300L))
        println(s"${Thread.currentThread.getName}: w:$i stop $out")
      }

  def flow: Stream[IO, Unit] = {
    val sName       = "scenario1"
    val delayPerMsg = 1L
    val window      = 5000L
    val sourceDelay = 5.millis

    val srcMessage  = "fs2_src_01:1|c"
    val sinkMessage = "fs2_sink_01:1|c"
    val gSrc        = graphiteInstance
    val gSink       = graphiteInstance

    (for {
      q ← Stream.eval(Queue.bounded[IO, Long](1 << 6))

      src = Stream
        .fixedRate[IO](sourceDelay)
        .scan(State(item = 0L))((acc, _) ⇒ tumblingWindow(acc, window))
        .through(graphitePipe(gSrc, srcMessage, { s ⇒
          s.item
        }))
        //.evalMap(state ⇒ send(monitoring, msg).map(_ ⇒ state.item))
        .through(q.enqueue)

      sink = q.dequeue
        .scan(0L)((acc, _) ⇒ slowDown(acc, delayPerMsg))
        .through(graphiteSink[Long](gSink, sinkMessage))
      //.evalMap(_ ⇒ send(monitoring, sinkMessage))

      out ← src mergeHaltL sink
    } yield out)
      .handleErrorWith(th ⇒ Stream.eval(IO(println(s"★ ★ ★  $sName error: ${th.getMessage}  ★ ★ ★"))))
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★  $sName completed ★ ★ ★"))))
  }

  def flow2 = {
    val window      = 5000L
    val sourceDelay = 50.millis
    val par         = 4

    val src = Stream
      .fixedRate[IO](sourceDelay)
      .scan(State[Long](item = 0L))((acc, _) ⇒ tumblingWindow(acc, window))
      .map(_.item)
      .take(200)

    //src.broadcastN(par, 1 << 5)(testSink(_))

    /*src.broadcastN2(par, 1 << 5)(i ⇒ IO {
      val ind = i % par
      println(s"${Thread.currentThread.getName}: worker:${ind} starts: $i")
      Thread.sleep(ThreadLocalRandom.current.nextInt(100, 500))
      println(s"${Thread.currentThread.getName}: worker:${ind} stop: $i")
      i
    })*/

    src.broadcastN3(par, 1 << 4) { ind: Int ⇒ (e) ⇒
      IO {
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
        .scan[Option[Long]](Some(0L))((a, _) ⇒ a.map(_ + 1L))
        .take(200)
        .through(q.enqueue)
        .onComplete(Stream.fixedRate[IO](500.millis).map(_ ⇒ None).through(q.enqueue))

      all = Seq(
        q.dequeue.unNoneTerminate.evalMap(worker(0)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(1)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(2)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(3)(_)),
        q.dequeue.unNoneTerminate.evalMap(worker(4)(_))
      )

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
            Thread.sleep(ThreadLocalRandom.current.nextLong(100L, 300L))
            println(s"${Thread.currentThread.getName}: w:$num stop $id")
          }
        case PoisonPill ⇒
          IO {
            println(s"${Thread.currentThread.getName}: got Quit $num")
            Thread.sleep(ThreadLocalRandom.current.nextLong(100L, 300L))
          } *> s.update(_ - 1).flatMap(_ ⇒ s.get.map(println(_)))
      }

  //Shouldn't lose messages here, as we  bufferSize == countdown(counter) + (delay == 200.millis) which is average latency
  def flow3 = {
    val bufferSize = 1 << 4
    (for {
      q           ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Event](bufferSize))
      interrupter ← Stream.eval(SignallingRef[IO, Int](bufferSize))

      src = Stream
        .fixedRate[IO](50.millis)
        .scan[Task](Task(0L))((a, _) ⇒ a.copy(a.id + 1L))
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
  def flow4 =
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Long](1 << 4))

      interrupter ← Stream.eval(SignallingRef[IO, Boolean](false))

      src = Stream
        .fixedRate[IO](50.millis)
        .scan[Long](0L)((a, _) ⇒ a + 1L)
        .take(200)
        .through(q.enqueue)
        .onComplete(Stream.eval(interrupter.set(true)))

      sinks = (q.dequeue.evalMap(worker(0)(_)) merge q.dequeue.evalMap(worker(1)(_)))
        .interruptWhen(interrupter)

      flow ← sinks mergeHaltBoth src
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★ Interrupter flow completed ★ ★ ★"))))

  //src.balanceN[Unit](1 << 5, 4)(mapAsyncUnordered[IO, Long, Unit](4)(testSink(_)))
  def mapAsyncUnordered[F[_], A, B](parallelism: Int)(f: A ⇒ F[B])(implicit F: Concurrent[F]): Pipe[F, A, B] =
    (inner: fs2.Stream[F, A]) ⇒ inner.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism)

  override def run(args: List[String]): IO[ExitCode] =
    //flow3.compile.drain.as(ExitCode.Success)
    //flow4.compile.drain.as(ExitCode.Success)
    //oneToManyFlow.compile.drain.as(ExitCode.Success)
    flow2.compile.drain.as(ExitCode.Success)
}
