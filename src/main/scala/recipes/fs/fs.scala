package recipes

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

import cats.effect.{Concurrent, IO, Timer}
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

    private val threadNumber       = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup

    override def newThread(r: Runnable) = {
      val t = new Thread(group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      t.setDaemon(true)
      t
    }
  }

  //https://medium.com/anyjunk/how-to-traverse-sequentially-a071afacc84d
  implicit class TraverseOps[F[_], A](fa: F[A]) {

    def seqTraverse[B, M[_]](f: A ⇒ M[B])(implicit T: cats.Traverse[F], M: cats.Monad[M]): M[F[B]] = {
      //def lift[B](a: A, f: A ⇒ M[B]): cats.free.Free[LazyEval, B] = cats.free.Free.liftF(LazyEval(a, f))
      case class LazyFunc[B](a: A, f: A ⇒ M[B]) {
        def run: M[B] = f(a)
      }

      /*val transformation: FunctionK[LazyEval, M] = new FunctionK[LazyEval, M] {
        def apply[A](a: LazyEval[A]): M[A] = a.apply
      }*/

      //We have built our series of instructions using traverse, and we haven’t yet executed anything.
      T.traverse(fa)(a ⇒ cats.free.Free.liftF(LazyFunc(a, f)))
        //The foldMap guarantees that each step of our free monad is only executed once the previous step is finished
        .foldMap(new cats.arrow.FunctionK[LazyFunc, M] {
          def apply[A](a: LazyFunc[A]): M[A] = a.run
        })

    }
  }

  /*
    The use case for `broadcastN` method is as follows:
      You have a queue of incoming payloads, each payload needs to be processed using some user provided function.
      The processing must be done sequentially for all payloads that belong to the same partition, but two payloads
      belonging to different partitions can be processed concurrently.
      Partition number = seqNum % parallelism
   */
  implicit class StreamOps[A](val source: Stream[IO, A]) extends AnyRef {

    /**
      * Decouples producer from consumer enabling them to operate at its own rate up to `bufferSize`.
      * If we hit `bufferSize`, then `enqueue` operation semantically blocks until there is a free space in the queue.
      */
    def throughBuffer[B](bufferSize: Int)(
      f: A ⇒ IO[B]
    )(implicit F: Concurrent[IO], T: Timer[IO]): Stream[IO, B] =
      Stream.eval(Queue.bounded[IO, Option[A]](bufferSize)).flatMap { q ⇒
        val p = source
          .map(Some(_))
          .through(q.enqueue)
          .onComplete(Stream.fixedRate[IO](10.millis).map(_ ⇒ None).through(q.enqueue))
          .drain
        val c = q.dequeue.unNoneTerminate.evalMap(f)

        /*
        val delayPerMsg = 10L
        q.dequeue.unNoneTerminate.scan(100L) { (latency, _) ⇒
          val updated = latency + delayPerMsg
          Thread.sleep(0 + (updated / 1000), (updated % 1000).toInt)
          updated
        }
         */

        //runs p in background
        c concurrently p
      }

    def balanceN[B](parallelism: Int, bufferSize: Int)(
      f: A ⇒ IO[B]
    )(implicit F: Concurrent[IO], T: Timer[IO]): Stream[IO, B] =
      Stream.eval(Queue.bounded[IO, Option[A]](bufferSize)).flatMap { q ⇒
        //val onClose = Stream.eval(close(parallelism, q))
        val onClose                  = Stream.fixedRate[IO](100.millis).map(_ ⇒ None).through(q.enqueue)
        val src: Stream[IO, Nothing] = source.map(Some(_)).through(q.enqueue).onComplete(onClose).drain
        val qSink: Stream[IO, B] = q.dequeue.unNoneTerminate
          .through(_.map(a ⇒ fs2.Stream.eval(f(a))).parJoin(parallelism))

        //wait for either completes which in our case should be the qSink, because the src never terminates
        val r: Stream[IO, B] = src.mergeHaltBoth(qSink)
        r
      }

    def balanceN2[B: HasLongHash: ClassTag](parallelism: Int, bufferSize: Int)(
      f: A ⇒ IO[B]
    )(implicit F: Concurrent[IO], T: Timer[IO]): Stream[IO, B] =
      Stream.eval(Queue.bounded[IO, Option[A]](bufferSize)).flatMap { q ⇒
        val h       = implicitly[HasLongHash[B]]
        val shards  = Vector.range(0, parallelism)
        val onClose = Stream.fixedRate[IO](100.millis).map(_ ⇒ None).through(q.enqueue)

        val qSrc: Stream[IO, Nothing] = source.map(Some(_)).through(q.enqueue).onComplete(onClose).drain
        val qSink: Stream[IO, B]      = q.dequeue.unNoneTerminate.through(_.evalMap(f))

        val zero = qSink.filter(h(_) % shards.size == shards.head)
        val sinks = shards.tail.foldLeft(zero) { (stream, ind) ⇒
          stream.merge(qSink.filter(h(_) % shards.size == ind))
        }

        //wait for either completes which in our case should be the sinks, because the src never terminates
        val r: Stream[IO, B] = qSrc.mergeHaltBoth(sinks)
        r
      }

    //looks like the most correct implementation from balanceN, balanceN2
    /**
      *
      */
    def balanceN3[B](parallelism: Int, bufferSize: Int)(
      f: Int ⇒ A ⇒ IO[B]
    )(implicit F: Concurrent[IO]): Stream[IO, B] = {
      import cats.implicits._
      val queues: IO[Vector[Queue[IO, Option[A]]]] =
        implicitly[cats.Traverse[Vector]]
          .traverse(Vector.range(0, parallelism))(_ ⇒ Queue.bounded[IO, Option[A]](bufferSize))

      Stream.eval(queues).flatMap { qs ⇒
        val sinks: Stream[IO, B] =
          Stream
            .emits(qs.zipWithIndex.map {
              case (q, ind) ⇒
                q.dequeue.unNoneTerminate.evalMap(f(ind))
            })
            .parJoin(parallelism)

        val balancedSrc: Stream[IO, Nothing] = source
            .mapAccumulate(-1L)((seqNum, elem) ⇒ (seqNum + 1L, elem))
            .evalMap { case (seqNum, elem) ⇒ qs((seqNum % parallelism).toInt).enqueue1(Some(elem)) }
            .drain ++
          Stream
            .emits(qs)
            .evalMap(_.enqueue1(None))
            .onComplete(Stream.eval(IO(println(" ★ ★ ★  Source is done   ★ ★ ★ "))))
            .drain

        //wait for both to exit
        balancedSrc merge sinks
      }
    }
  }
}
