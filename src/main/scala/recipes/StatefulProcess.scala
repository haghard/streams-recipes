package recipes

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{ActorSystem, Scheduler}
import akka.stream.QueueOfferResult.{Dropped, Enqueued}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.{ActorAttributes, ActorMaterializer, Materializer, OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{FlowWithContext, Keep, Sink, Source}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

//runMain StatefulProcess
object StatefulProcess {

  object TimedPromise {
    final case class PromiseExpired(timeout: FiniteDuration)
        extends Exception(s"Promise not completed within $timeout!")

    def apply[A](timeout: FiniteDuration)(implicit ec: ExecutionContext, scheduler: Scheduler): Promise[A] = {
      val promise = Promise[A]()
      val expired = akka.pattern.after(timeout, scheduler)(Future.failed(PromiseExpired(timeout)))
      promise.completeWith(expired)
      promise
    }
  }

  case class ProcessorUnavailable(name: String)
      extends Exception(s"Processor $name cannot accept requests at this time!")

  case class ProcessorError(result: QueueOfferResult) extends Exception(s"Unexpected queue offer result: $result!")

  trait Cmd

  case class AddUser(id: Long) extends Cmd

  case class RmUser(id: Long) extends Cmd

  trait Evn

  trait Reply

  case class Added(id: Long) extends Reply

  case class Removed(id: Long) extends Reply

  case class UserState(users: Set[Long] = Set.empty, current: Long = -1L, p: Promise[Seq[Reply]] = null)

  def stateFlow(
    userState: UserState
  )(implicit ec: ExecutionContext): FlowWithContext[Cmd, Promise[Seq[Reply]], Seq[Reply], Promise[Seq[Reply]], Any] = {
    /*FlowWithContext[Cmd, Promise[Seq[Reply]]].map {
      case AddUser(id) ⇒ Seq(Added(id))
      case RmUser(id)  ⇒ Seq(Removed(id))
    }*/

    val f = FlowWithContext[Cmd, Promise[Seq[Reply]]].asFlow
      .scan(userState) { case (state, (cmd, p)) ⇒
        cmd match {
          case AddUser(id) ⇒ state.copy(state.users + id, id, p)
          case RmUser(id)  ⇒ state.copy(state.users - id, id, p)
        }
      }
      /*.map { state ⇒
        if (state.current != -1L) (Seq(Added(state.current)), state.p)
        else (Seq.empty, state.p)
      }*/
      .mapAsync(1)(state ⇒ persist(state.current).map(r ⇒ (r, state.p)))

    FlowWithContext.fromTuples(f)
  }

  def persist(userId: Long)(implicit ec: ExecutionContext) =
    Future {
      println(s"persist: ${userId}")
      Thread.sleep(ThreadLocalRandom.current.nextInt(50, 100))
      if (userId != -1L) Seq(Added(userId)) else Seq.empty
    }

  def main(args: Array[String]): Unit = {
    println("********************************************")

    implicit val sys: ActorSystem  = ActorSystem("streams")
    implicit val mat: Materializer = ActorMaterializer()

    implicit val sch = sys.scheduler
    implicit val ec  = mat.executionContext

    val processor =
      Source
        .queue[(Cmd, Promise[Seq[Reply]])](1 << 2, OverflowStrategy.dropNew)
        .via(stateFlow(UserState()))
        .toMat(Sink.foreach { case (replies, p) ⇒
          println("replies: " + replies)
          p.trySuccess(replies)
        })(Keep.left)
        //.withAttributes(ActorAttributes.supervisionStrategy(akka.stream.Supervision.resumingDecider))
        .addAttributes(ActorAttributes.supervisionStrategy(akka.stream.Supervision.resumingDecider))
        .run()

    val f = produce(0L, processor)
      .flatMap { _ ⇒
        /*Thread.sleep(3000); */
        sys.terminate
      }
    Await.result(f, Duration.Inf)
  }

  def produce(id: Long, processor: SourceQueueWithComplete[(Cmd, Promise[Seq[Reply]])])(implicit
    ec: ExecutionContext,
    sch: Scheduler
  ): Future[Long] =
    if (id <= 100)
      Future
        .traverse(Seq(id, id + 1, id + 2, id + 3)) { id ⇒
          val p = TimedPromise[Seq[Reply]](300.millis)
          processor
            .offer((AddUser(id), p))
            .flatMap {
              case Enqueued ⇒
                p.future
                  .map(_ ⇒ id)
                  .recoverWith { case err: Throwable ⇒
                    println("Time-out: " + id + ":" + err.getMessage)
                    akka.pattern.after(500.millis, sch)(produce(id, processor))
                  }
              case Dropped ⇒
                println(s"Dropped: $id")
                akka.pattern.after(500.millis, sch)(produce(id, processor))
              //Future.failed(ProcessorUnavailable("Unavailable"))
              case other ⇒
                println(s"Failed: $id")
                Future.failed(ProcessorError(other))
            }
        }
        .flatMap(ids ⇒ produce(ids.max, processor))
    else Future.successful(id)
}
