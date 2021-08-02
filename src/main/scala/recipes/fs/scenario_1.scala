package recipes.fs

import java.util.concurrent.{Executors, ThreadLocalRandom}

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect._
import cats.implicits._
import fs2._
import fs2.concurrent.{Queue, Signal, SignallingRef}
import recipes.{GraphiteSupport, StatsDMetrics, TimeWindows}

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import recipes.fs._

/*

https://fs2.io/concurrency-primitives.html
https://www.beyondthelines.net/programming/streaming-patterns-with-fs2/

runMain recipes.fs.scenario_1

 */
object scenario_1 extends IOApp with TimeWindows with GraphiteSupport {

  val parallelism                   = 4
  implicit val ec                   = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism, FsDaemons("scenario01")))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val t                    = cats.effect.IO.timer(ec)
  //val C = Concurrent[IO]

  def writeToDB[F[_]: Async](chunk: Chunk[Int]): F[Unit] =
    Async[F].async { cb ⇒
      println(s"Writing batch of $chunk to database by ${Thread.currentThread.getName}")
      cb(Right(()))
    }

  /*
  import fs2.interop.reactivestreams._
  //To convert a Stream into a downstream unicast org.reactivestreams.Publisher:
  val pub: fs2.interop.reactivestreams.StreamUnicastPublisher[IO, Long] =
    Stream.emits(1L to 100L).covary[IO].toUnicastPublisher()
  //To convert an upstream org.reactivestreams.Publisher into a Stream:
  pub.toStream[IO]

  akka.stream.scaladsl.Source.fromPublisher(pub)
   */

  //type Pipe[F[_], -I, +O] = Stream[F, I] => Stream[F, O]
  def pipe2Graphite[F[_]: Sync, T](
    monitoring: StatsDMetrics,
    message: String
  ): Pipe[F, T, T] /*Stream[F, T] => Stream[F, T]*/ =
    _.evalTap { _ ⇒
      Sync[F].delay(send(monitoring, message))
    //Sync[F].delay(println(s"Stage $name processing $i by ${Thread.currentThread.getName}"))
    }

  def pipe2GraphitePar[F[_]: Sync: LiftIO, T](
    monitoring: StatsDMetrics,
    message: String
  ): Stream[F, T] ⇒ Stream[F, T] =
    _.evalTap(_ ⇒ (IO.shift *> IO(send(monitoring, message))).runAsync(_ ⇒ IO.unit).to[F])

  def stop[T](in: Stream[IO, T]): Stream[IO, T] = {
    val signal: Stream[IO, Boolean] = Signal.constant[IO, Boolean](false).discrete
    in.interruptWhen(signal)
  }

  def source(
    latency: FiniteDuration,
    timeWindow: Long,
    msg: String,
    monitoring: StatsDMetrics,
    q: Queue[IO, Long]
  ): Stream[IO, Unit] =
    Stream
      .fixedRate[IO](latency)
      .scan(State(item = 0L))((acc, _) ⇒ tumblingWindow(acc, timeWindow))
      .through(graphitePipe(monitoring, msg, s ⇒ s.item))
      //.evalMap(state ⇒ send(monitoring, msg).map(_ ⇒ state.item))
      .through(q.enqueue)

  def graphiteSink[A](monitoring: StatsDMetrics, message: String): Pipe[IO, A, Unit] =
    _.evalMap(_ ⇒ send(monitoring, message))

  def graphitePipe[A, B](monitoring: StatsDMetrics, message: String, f: A ⇒ B): Pipe[IO, A, B] =
    _.evalMap(i ⇒ send(monitoring, message).map(_ ⇒ f(i)))

  def testSink(i: Long) =
    IO {
      println(s"${Thread.currentThread.getName}: $i start")
      Thread.sleep(ThreadLocalRandom.current.nextInt(300, 500))
      println(s"${Thread.currentThread.getName}: $i stop")
      i
    }

  def ts[T](i: T) =
    IO {
      println(s"${Thread.currentThread.getName}: $i start")
      Thread.sleep(ThreadLocalRandom.current.nextInt(300, 500))
      println(s"${Thread.currentThread.getName}: $i stop")
      i
    }

  def worker(i: Int): Long ⇒ IO[Unit] =
    (out: Long) ⇒
      IO[Unit] {
        println(s"${Thread.currentThread.getName}: w:$i start $out")
        Thread.sleep(ThreadLocalRandom.current().nextLong(100L, 300L))
        println(s"${Thread.currentThread.getName}: w:$i stop $out")
      }

  def worker[T](i: T): IO[T] =
    IO[T] {
      println(s"${Thread.currentThread.getName}: start $i")
      Thread.sleep(500)
      //Thread.sleep(ThreadLocalRandom.current().nextLong(100L, 300L))
      println(s"${Thread.currentThread.getName}: stop $i")
      i
    }

  /*
    The setup of the scenario is as follows:
      The source and sink run at the same rate at the beginning.
      The sink slows down over time, increasing latency on every message.
      I use the bounded queue between the source and the sink.
      This leads to blocking "enqueue" operation for the source in case no space in the queue.
      Result: The source's rate is going to decrease proportionally to the sink's rate.
   */
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

      src =
        Stream
          .fixedRate[IO](sourceDelay)
          .scan(State(item = 0L))((acc, _) ⇒ tumblingWindow(acc, window))
          .through(graphitePipe(gSrc, srcMessage, s ⇒ s.item))
          //.evalMap(state ⇒ send(monitoring, msg).map(_ ⇒ state.item))
          .through(q.enqueue)

      sink =
        q.dequeue
          .scan(0L)((acc, _) ⇒ slowDown(acc, delayPerMsg))
          .through(graphiteSink[Long](gSink, sinkMessage))
      //.evalMap(_ ⇒ send(monitoring, sinkMessage))

      out ← src mergeHaltL sink
    } yield out)
      .handleErrorWith(th ⇒ Stream.eval(IO(println(s"★ ★ ★  $sName error: ${th.getMessage}  ★ ★ ★"))))
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★  $sName completed ★ ★ ★"))))
  }

  /*
    The setup of the scenario is as follows:
      We have one source and `parallelism` number of sinks. It runs in fan-in fashion using `parallelism` number of queues
      upfront every sink. The sinks run concurrently.

      The source and sink run at the same rate at the beginning.
      The sink slows down over time, increasing latency on every message.
      I use the bounded queue between the source and the sink.
      This leads to blocking "enqueue" operation for the source in case no space in the queue.
      Result: The source's rate is going to decrease proportionally to the sink's rate.
   */
  def flow2[B](worker: Long ⇒ IO[B]): Stream[IO, B] = {
    val window      = 5000L
    val sourceDelay = 50.millis

    val src = Stream
      .fixedRate[IO](sourceDelay)
      .scan(State[Long](item = 0L))((acc, _) ⇒ tumblingWindow(acc, window))
      .map(_.item)
      //.prefetchN(16)
      .take(100)

    //src.broadcastN(par, 1 << 5)(testSink(_))

    /*src.broadcastN2(par, 1 << 5)(i ⇒ IO {
      val ind = i % par
      println(s"${Thread.currentThread.getName}: worker:${ind} starts: $i")
      Thread.sleep(ThreadLocalRandom.current.nextInt(100, 500))
      println(s"${Thread.currentThread.getName}: worker:${ind} stop: $i")
      i
    })*/

    src.balanceN3(parallelism, 1 << 3) { i: Int ⇒ worker }
  }

  //static number of workers
  def oneToManyFlow: Stream[IO, Unit] =
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Option[Long]](1 << 3))
      src =
        Stream
          .fixedRate[IO](50.millis)
          //.balance()
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

  def sink[T](num: Int, s: SignallingRef[IO, Int]): Event ⇒ IO[Unit] =
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
  //One to many semantic. One queue and static number of sinks.
  def flow3: Stream[IO, Unit] = {
    val bufferSize = 1 << 4
    (for {
      q           ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Event](bufferSize))
      interrupter ← Stream.eval(SignallingRef[IO, Int](bufferSize))

      src =
        Stream
          .fixedRate[IO](50.millis)
          .scan[Task](Task(0L))((a, _) ⇒ a.copy(a.id + 1L))
          .take(200)
          .through(q.enqueue)
          .onComplete(Stream.fixedRate[IO](250.millis).map(_ ⇒ PoisonPill).through(q.enqueue))
          .interruptWhen(interrupter.discrete.map(_ <= 0))

      sinks = q.dequeue.evalMap(sink(0, interrupter)(_)) merge q.dequeue.evalMap(sink(1, interrupter)(_))

      flow ← sinks mergeHaltBoth src
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★ Interrupt completed ★ ★ ★"))))
  }

  //we lose messages when stop the sink
  def flow4: Stream[IO, Unit] =
    (for {
      q ← Stream.eval(fs2.concurrent.Queue.bounded[IO, Long](1 << 4))

      interrupter ← Stream.eval(SignallingRef[IO, Boolean](false))

      src =
        Stream
          .fixedRate[IO](50.millis)
          .scan[Long](0L)((a, _) ⇒ a + 1L)
          .take(200)
          .through(q.enqueue)
          .onComplete(Stream.eval(interrupter.set(true)))

      sinks = (q.dequeue.evalMap(worker(0)(_)) merge q.dequeue.evalMap(worker(1)(_)))
        .interruptWhen(interrupter)

      flow ← sinks mergeHaltBoth src
    } yield flow)
      .onComplete(fs2.Stream.eval(IO(println(s"★ ★ ★ Interrupt completed ★ ★ ★"))))

  //Stream.emits(1L to 200L).covary[IO]
  def flow5[B](worker: Long ⇒ IO[B]): Stream[IO, B] =
    Stream
      .fixedRate[IO](10.millis)
      .scan(State[Long](item = 0L))((acc, _) ⇒ tumblingWindow(acc, 5000L))
      .map(_.item)
      .take(100)
      .balance(1)
      .take(parallelism)
      .map(_.through(_.evalMap(worker)))
      .parJoinUnbounded

  //src.balanceN[Unit](1 << 5, 4)(mapAsyncUnordered[IO, Long, Unit](4)(testSink(_)))
  def mapAsyncUnordered[F[_], A, B](parallelism: Int)(f: A ⇒ F[B])(implicit F: Concurrent[F]): Pipe[F, A, B] =
    (inner: fs2.Stream[F, A]) ⇒ inner.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism)

  //override protected implicit def contextShift: ContextShift[IO] =

  override def run(args: List[String]): IO[ExitCode] = {
    println("run")

    //flow3.compile.drain.as(ExitCode.Success)
    //flow4.compile.drain.as(ExitCode.Success)
    //oneToManyFlow.compile.drain.as(ExitCode.Success)

    /*Await.result(
      List
        .range(1, 10)
        .seqTraverse(
          i ⇒
            Future {
              println(s"${Thread.currentThread.getName}: start $i")
              Thread.sleep(ThreadLocalRandom.current.nextLong(100L, 200L))
              println(s"${Thread.currentThread.getName}: stop $i")
              Option(i)
            }
        ),
      Duration.Inf
    )
    IO(ExitCode.Success)
     */

    //fs2.Stream.range(1, 100)

    flow2(worker(_)).compile
      .foldMonoid(cats.Monoid[Long])
      .redeem(
        { er ⇒
          println("Error:" + er)
          ExitCode.Error
        },
        { r ⇒
          println("res:" + r)
          ExitCode.Success
        }
      )

    //or
    /*
    flow5(worker(_)).compile
      .foldMonoid(cats.Monoid[Long])
      .redeem({ er ⇒
        println("Error:" + er)
        ExitCode.Error
      }, { r ⇒
        println("res:" + r)
        ExitCode.Success
      })*/
  }

}
