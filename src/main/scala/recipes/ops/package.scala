package recipes

import cats.effect.{ Effect, IO }
import fs2.Pipe
import fs2.async.mutable

import scala.concurrent.ExecutionContext

package object ops {

  implicit class StreamOps[A](val source: fs2.Stream[IO, A]) {
    private def close(n: Int, q: mutable.Queue[IO, Option[A]]): IO[Unit] = {
      println("close")
      if (n == 1) q.enqueue1(None)
      else (0 to n).map(_ ⇒ q.enqueue1(None)).head
    }

    def balance[B](qSize: Int, parallelism: Int)(sink: Pipe[IO, A, B])(implicit F: Effect[IO], ex: ExecutionContext): fs2.Stream[IO, B] = {
      fs2.Stream.eval(fs2.async.boundedQueue[IO, Option[A]](qSize)).flatMap { q ⇒
        val cb = fs2.Stream.eval(close(parallelism, q))
        val src: fs2.Stream[IO, Nothing] =
          source.map(Some(_)).to(q.enqueue).onComplete[Unit](cb).drain
        val sink0: fs2.Stream[IO, B] = q.dequeue.unNoneTerminate.through(sink)

        val r: fs2.Stream[IO, B] = src.concurrently[B](sink0)
        r
      }
    }
  }
}