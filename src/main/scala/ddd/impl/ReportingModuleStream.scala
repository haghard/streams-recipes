package ddd.impl

import scalaz._
import Scalaz._

import ddd.repo.AccountRepo
import ddd.services.ReportingModule
import recipes.ScalazRecipes.RecipesDaemons

object ReportingModuleStream {
  //({ type λ[x] = scalaz.stream.Process[scalaz.concurrent.Task, x] })#λ
  type PTask[X] = scalaz.stream.Process[scalaz.concurrent.Task, X]
}

//trait ReportingModuleStream extends ReportingModule[({ type λ[x] = scalaz.stream.Process[scalaz.concurrent.Task, x] })#λ] {
trait ReportingModuleStream extends ReportingModule[ReportingModuleStream.PTask] {
  mixin: { def executor: java.util.concurrent.ExecutorService } ⇒

  override type T = (String, ddd.Amount)

  override def balances: ReportOperation[Seq[T]] =
    scalaz.Kleisli.kleisli[ReportingModuleStream.PTask, AccountRepo, ddd.Valid[Seq[T]]] { repo: AccountRepo ⇒
      scalaz.stream.Process.eval {
        scalaz.concurrent.Task {
          repo.all.fold({ error ⇒ error.toString().failureNel }, { as: Seq[ddd.account.Account] ⇒
            as.map(a ⇒ (a.no, a.balance.amount)).success
          })
        }(executor)
      }
    }
}

object ReportingStreamingService extends ReportingModuleTask {
  val executor = java.util.concurrent.Executors.newFixedThreadPool(2, new RecipesDaemons("reporting-streams"))
}
