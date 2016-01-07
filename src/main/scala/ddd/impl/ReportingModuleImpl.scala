package ddd.impl

import ddd.Amount
import ddd.account.Account
import ddd.repo.AccountRepo
import ddd.services.ReportingModule
import recipes.ScalazRecipes.RecipesDaemons

import scalaz._
import Scalaz._
import scalaz.Kleisli._
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingModuleImpl extends ReportingModule[Task] {
  mixin: { def executor: java.util.concurrent.ExecutorService } ⇒

  override type T = ddd.Amount

  private val P = Process

  override def balances: ReportOperation[Seq[(String, Amount)]] =
    kleisli[Task, AccountRepo, ddd.Valid[Seq[(String, Amount)]]] { repo: AccountRepo ⇒
      Task {
        repo.all.fold({ error ⇒ error.toString().failureNel }, { as: Seq[Account] ⇒
          as.map(a ⇒ (a.no, a.balance.amount)).success
        })
      }(executor)
    }

  override def balancesProcess: Reader[AccountRepo, Process[Task, ddd.Valid[Seq[(String, Amount)]]]] = ???
  /*
    Reader { repo ⇒
        P.eval(Task.delay(repo)).map(balances.run(_))
          .onFailure { ex: Throwable ⇒
            P.eval(Task.delay(ex.getMessage.failureNel))
          }
      }*/
}

object ReportingService extends ReportingModuleImpl {
  val executor = java.util.concurrent.Executors.newFixedThreadPool(2, new RecipesDaemons("reporting"))
}