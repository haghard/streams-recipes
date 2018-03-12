package ddd.impl

import ddd.account.Account
import ddd.repo.AccountRepo
import ddd.services.ReportingModule
import recipes.ScalazRecipes.RecipesDaemons

import scalaz._
import Scalaz._
import scalaz.Kleisli._
import scalaz.concurrent.Task

trait ReportingModuleTask extends ReportingModule[Task] {
  mixin: { def executor: java.util.concurrent.ExecutorService } ⇒

  override type T = (String, ddd.Amount)

  override def balances: ReportOperation[Seq[T]] =
    kleisli[Task, AccountRepo, ddd.Valid[Seq[T]]] { repo: AccountRepo ⇒
      Task {
        repo.all.fold({ error ⇒
          error.toString().failureNel
        }, { as: Seq[Account] ⇒
          as.map(a ⇒ (a.no, a.balance.amount)).success
        })
      }(executor)
    }
}

object ReportingService extends ReportingModuleTask {
  val executor = java.util.concurrent.Executors
    .newFixedThreadPool(2, new DddDaemons("reporting"))
}
