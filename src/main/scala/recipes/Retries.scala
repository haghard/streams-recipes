package recipes

/*
import fs2._
import java.time.Instant
import scala.concurrent.duration._

/**
 *
 *
 * That example is kind of interesting in that it takes a Stream[Task, Task[A]] and evaluates each task at least once, but if there are no tasks
 * available and the last task failed with an exception, it is retried until it succeeds or until a new task shows up in the source stream
 *
 *
 * If you don't need to retry, then by far the simplest option is to use .attempt on the Task
 * that does the publishing, then something like
 * .flatMap {
 *   case Left(t) => Stream.eval_(Task.delay(logError("Failed to publish ...")));
 *   case Right(_) => Stream.empty
 * }
 *
 */

//https://gist.github.com/mpilquist/21430db54b22c17557a63a15d7b4be06#file-retries-scala
//TODO: to try it in action


object Retries {

  /**
 * Returns a stream that evaluates the specified task once and then for each unit that appears in the `changes` stream. If any task evaluation
 * fails with an exception, the task is retried according to the specified `retryDelay` schedule until it succeeds or a value from the `changes`
 * stream appears.
 *
 * @param task       task to evaluate
 * @param changes    stream that signals the task should be evaluated again (or if currently retrying, the retry delay should be reset)
 * @param retryDelay function which determines how long to wait before retrying the task after the specified evaluation attempt number
 */
  def retryTaskOnFailureAndChange[A](task: Task[A], changes: Stream[Task, Unit], retryDelay: Int ⇒ FiniteDuration)(implicit strategy: Strategy, scheduler: Scheduler): Stream[Task, Either[Throwable, A]] = {
    val firstAndChanges: Stream[Task, Task[A]] = (Stream.emit(()) ++ changes).map(_ ⇒ task)
    retryTasksOnFailure(firstAndChanges, retryDelay)
  }

  /**
 * Returns a stream that evaluates the specified stream of tasks. Each task is evaluated at least once. If evaluation of a task fails, and
 * the next task is not yet available from the `tasks` stream, then the failed task is retried according to the retry schedule specified by
 * the `retryDelay` function.
 *
 * @param tasks      tasks to evaluate
 * @param retryDelay function which determines how long to wait before retrying the task after the specified evaluation attempt number
 */
  def retryTasksOnFailure[A](tasks: Stream[Task, Task[A]], retryDelay: Int ⇒ FiniteDuration)(implicit strategy: Strategy, scheduler: Scheduler): Stream[Task, Either[Throwable, A]] = {
    Stream.eval(async.synchronousQueue[Task, Unit]).flatMap { clockTicksQueue ⇒
      (clockTicksQueue.dequeue either tasks.through(signalize)).through(retryOnFailureAndChangePipe(retryDelay, clockTicksQueue.enqueue1(())))
    }
  }

  private def retryOnFailureAndChangePipe[A](retryDelay: Int ⇒ FiniteDuration, signalTick: Task[Unit])(implicit strategy: Strategy, scheduler: Scheduler): Pipe[Task, Either[Unit, Task[A]], Either[Throwable, A]] = {
    def waitingForTask: Handle[Task, Either[Unit, Task[A]]] ⇒ Pull[Task, Either[Throwable, A], Unit] = {
      _.receive1 {
        case (Right(task), h) ⇒
          attempt(task.attempt, 0)(h)
        case (Left(tick), h) ⇒
          waitingForTask(h)
      }
    }

    def attempt(task: Task[Either[Throwable, A]], attempts: Int): Handle[Task, Either[Unit, Task[A]]] ⇒ Pull[Task, Either[Throwable, A], Unit] = h ⇒ {
      Pull.eval(task).flatMap {
        case e @ Right(a) ⇒ Pull.output1(e) >> waitingForTask(h)
        case e @ Left(t) ⇒
          Pull.output1(e) >> {
            val attempt = attempts + 1
            val delayBeforeNextAttempt = retryDelay(attempt)
            for {
              nextAttempt ← Pull.eval(Task.delay(Instant.now.toEpochMilli + delayBeforeNextAttempt.toMillis))
              _ ← Pull.eval(Task.start(signalTick.schedule(delayBeforeNextAttempt)))
              result ← retrying(task, attempts + 1, nextAttempt)(h)
            } yield result
          }
      }
    }

    def retrying(task: Task[Either[Throwable, A]], attempts: Int, nextAttemptMillis: Long): Handle[Task, Either[Unit, Task[A]]] ⇒ Pull[Task, Either[Throwable, A], Unit] = h ⇒ {
      h.receive1 {
        case (Right(task), h) ⇒
          waitingForTask(h.push(Chunk.singleton(Right(task))))
        case (Left(tick), h) ⇒
          Pull.eval(Task.delay(Instant.now)).flatMap { now ⇒
            if (now.toEpochMilli < nextAttemptMillis) retrying(task, attempts, nextAttemptMillis)(h)
            else attempt(task, attempts)(h)
          }
      }
    }

    _.pull(waitingForTask)
  }

  /** Returns a stream that outputs the latest available `A` from the input. */
  def signalize[F[_]: util.Async, A]: Pipe[F, A, A] = in ⇒ {
    Stream.eval(async.signalOf[F, Option[A]](None)).flatMap { signal ⇒
      in.evalMap(a ⇒ signal.set(Some(a))).drain merge signal.discrete.collect { case Some(a) ⇒ a }
    }
  }
}
 */
