package ddd.impl

import scalaz._
import Scalaz._
import cats.effect.{ContextShift, IO}
import ddd.repo.AccountRepo
import ddd.services.ReportingModule
import cats.implicits._

import scala.concurrent.ExecutionContext

object ReportingModuleStream {
  //({ type λ[x] = scalaz.stream.Process[scalaz.concurrent.Task, x] })#λ
  //type PTask[X] = scalaz.stream.Process[scalaz.concurrent.Task, X]
  type PTask[X] = fs2.Stream[cats.effect.IO, X] //scalaz.concurrent.Task
}

//trait ReportingModuleStream extends ReportingModule[({ type λ[x] = scalaz.stream.Process[scalaz.concurrent.Task, x] })#λ] {
trait ReportingModuleStream extends ReportingModule[ReportingModuleStream.PTask] {
  mixin: { def executor: java.util.concurrent.ExecutorService } ⇒

  override type T = (String, ddd.Amount)

  implicit lazy val cs: ContextShift[IO] =
    IO.contextShift(ExecutionContext.fromExecutor(executor))

  override def balances: ReportOperation[Seq[T]] =
    scalaz.Kleisli
      .kleisli[ReportingModuleStream.PTask, AccountRepo, ddd.Valid[Seq[T]]] { repo: AccountRepo ⇒
        fs2.Stream.eval {
          IO.shift *> IO {
            repo.all.fold(
              error ⇒ error.toString().failureNel,
              { as: Seq[ddd.account.Account] ⇒
                as.map(a ⇒ (a.no, a.balance.amount)).success
              }
            )
          }
        }
      }
}

object ReportingStreamingService extends ReportingModuleTask {
  val executor = java.util.concurrent.Executors
    .newFixedThreadPool(2, DddDaemons("reporting-streams"))
}
