package recipes

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{ Concurrent, IO, Timer }
import fs2.Stream
import fs2.concurrent.Queue

import scala.reflect.ClassTag
import scala.concurrent.duration._

package object fs {

  trait HasLongHash[T] {
    def apply(v: T): Long
  }

  implicit val state2Long = new HasLongHash[State[Long]] {
    def apply(v: State[Long]): Long = v.item
  }

  implicit val long2long = new HasLongHash[Long] {
    def apply(v: Long): Long = v
  }

  case class State[T: HasLongHash: ClassTag](item: T, ts: Long = System.currentTimeMillis, count: Long = 0)

  case class FsDaemons(name: String) extends ThreadFactory {
    private def namePrefix = s"$name-thread"

    private val threadNumber = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = {
      val t = new Thread(group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      t.setDaemon(true)
      t
    }
  }

  implicit class StreamOps[A](val source: Stream[IO, A]) {

    def balanceN[B](parallelism: Int, bufferSize: Int)(f: A ⇒ IO[B])(implicit F: Concurrent[IO], T: Timer[IO]): Stream[IO, B] = {
      Stream.eval(Queue.bounded[IO, Option[A]](bufferSize)).flatMap { q ⇒
        //val onClose = Stream.eval(close(parallelism, q))
        val onClose = Stream.fixedRate[IO](100.millis).map(_ ⇒ None).through(q.enqueue)
        val src: Stream[IO, Nothing] = source.map(Some(_)).through(q.enqueue).onComplete(onClose).drain
        val qSink: Stream[IO, B] = q.dequeue
          .unNoneTerminate
          .through(_.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism))

        //wait for either completes, which in our case should be the qSink, because the src never terminates
        val r: Stream[IO, B] = src.mergeHaltBoth(qSink)
        r
      }
    }

    def balanceN2[B: HasLongHash: ClassTag](parallelism: Int, bufferSize: Int)(f: A ⇒ IO[B])(implicit F: Concurrent[IO], T: Timer[IO]): Stream[IO, B] = {
      Stream.eval(Queue.bounded[IO, Option[A]](bufferSize)).flatMap { q ⇒
        val h = implicitly[HasLongHash[B]]
        val shards = Vector.range(0, parallelism)
        val onClose = Stream.fixedRate[IO](100.millis).map(_ ⇒ None).through(q.enqueue)

        val qSrc: Stream[IO, Nothing] = source.map(Some(_)).through(q.enqueue).onComplete(onClose).drain
        val qSink: Stream[IO, B] = q.dequeue.unNoneTerminate.through(_.evalMap(f))

        val zero = qSink.filter(h(_) % shards.size == shards.head)
        val sinks = shards.tail.foldLeft(zero) { (stream, ind) ⇒
          stream.merge(qSink.filter(h(_) % shards.size == ind))
        }

        //wait for either completes, which in our case should be the sinks, because the src never terminates
        val r: Stream[IO, B] = qSrc.mergeHaltBoth(sinks)
        r
      }
    }
  }
}
