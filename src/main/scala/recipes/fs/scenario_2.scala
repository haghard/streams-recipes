package recipes.fs

import java.util.concurrent.{Executors, ThreadLocalRandom}

import cats.effect.{Async, Concurrent, ContextShift, ExitCode, IO, IOApp, LiftIO, Sync}
import fs2.{Chunk, Pipe, Pull, Stream}
import recipes.{GraphiteSupport, TimeWindows}

import scala.concurrent.ExecutionContext
import cats.implicits._

import scala.concurrent.duration._

/*
https://youtu.be/5TR89KzPaJ4?list=PLbZ2T3O9BuvczX5j03bWMrMFzK5OAs9mZ

runMain recipes.fs.scenario_2
 */
object scenario_2 extends IOApp with TimeWindows with GraphiteSupport {
  val parallelism = 4

  implicit val ec                   = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(parallelism, FsDaemons("scenario02")))
  implicit val cs: ContextShift[IO] = IO.contextShift(ec)
  implicit val t                    = cats.effect.IO.timer(ec)

  val C = Concurrent[IO]

  /*class MyResource {
    def close: IO[Unit] = IO.delay()
  }

  val bracket = fs2.Stream.bracket(
    IO(new MyResource())
  )(r ⇒ r.close)
   */

  def pipeAsync[F[_]: Sync: LiftIO]: Stream[F, Long] ⇒ Stream[F, Long] =
    _.evalTap { i ⇒
      (IO.shift *> IO.sleep(100.millis) *> IO(i))
        .runAsync {
          case Left(e) ⇒
            IO.raiseError(e)
          case Right(r) ⇒
            IO(println(s"[${Thread.currentThread.getName}]: $r"))
        }
        .to[F]
    //.runAsync(_ ⇒ IO.unit).to[F]
    }

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

  def evalMapAsync[F[_]: Async, T: cats.Monoid](chunk: Chunk[T]): F[T] =
    Async[F].async { cb ⇒
      val r = chunk.foldLeft(cats.Monoid[T].empty)(_ |+| _)
      Thread.sleep(500)
      //if (ThreadLocalRandom.current().nextDouble > .8) throw new Exception("Boom !!!")
      println(s"${Thread.currentThread.getName} chunk sum:${r}")
      cb(Right(r))
    }

  override def run(args: List[String]): IO[ExitCode] = {
    println("run02")

    //fs2.Stream.emits(1L to 100L).covary[IO].through(pipeAsync[IO]).compile.drain.unsafeRunSync
    //fs2.Stream.emits(1L to 100L).through(sumEvery(10)).covary[IO].compile.foldMonoid(cats.Monoid[Long]).unsafeRunSync()
    //fs2.Stream.emits(1l to 100l).through(sumEvery(10)).covary[IO].compile.drain.unsafeRunSync()
    //fs2.Stream.emits(1l to 100l).through(sumEvery(10)).covary[IO].compile.toList.unsafeRunSync()

    //IO(ExitCode.Success)

    /*Stream
      .emits(1L to 100L)
      .covary[IO]
      .chunkN(10, true)
      .parEvalMap(4)(evalMapAsync[IO, Long])
      .compile
      .drain
      .unsafeRunAsync(_ ⇒ ())
     */

    //Stream.emits(1L to 200L).covary[IO].prefetchN()

    //Stream.emits(1L to 40L).covary[IO]

    //val io =
    /*fs2.Stream
      .repeatEval(IO {
        Thread.sleep(50)
        ThreadLocalRandom.current.nextLong(2000L)
      }) //.metered(50.millis)
      .take(50)
      .throughBuffer(1 << 3) { i ⇒
        IO {
          println(s"${Thread.currentThread.getName}: $i start")
          Thread.sleep(500) //ThreadLocalRandom.current.nextInt(1000, 2000)
          println(s"${Thread.currentThread.getName}: $i stop")
          i / 2
        }
      }
      .compile
      .foldMonoid(cats.Monoid[Long])*/

    //Stream.emits(1L to 100L).covary[IO].prefetchN(10)

    val io =
      Stream
        .emits(1L to 100L)
        .covary[IO]
        .balance(1)
        .take(parallelism)
        .map(
          _.through(
            _.evalMap(
              i ⇒
                IO {
                  println(s"${Thread.currentThread.getName}: $i start")
                  Thread.sleep(500) //ThreadLocalRandom.current.nextInt(1000, 2000)
                  println(s"${Thread.currentThread.getName}: $i stop")
                  i / 2
                }
            )
          )
        )
        .parJoinUnbounded
        .compile
        .foldMonoid(cats.Monoid[Long])

    /*Stream
      .emits(1L to 100L)
      .covary[IO]
      //.metered(100.millis)
      .chunkN(10, true)
      .parEvalMap(parallelism)(evalMapAsync[IO, Long])
      .compile
      .foldMonoid(cats.Monoid[Long])*/
    //.unsafeRunAsync { r ⇒ println(s"final sum:$r"); }

    io.redeem({ ex ⇒
      println("Error: " + ex.getMessage)
      ExitCode.Error
    }, { r ⇒
      println(s"Final sum:$r")
      ExitCode.Success
    })
  }
}
