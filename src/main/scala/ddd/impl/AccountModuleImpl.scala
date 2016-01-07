package ddd.impl

import ddd.repo.AccountRepo
import ddd.{ Balance, Amount }
import ddd.account.{ Checking, Savings, AccountType, Account }
import ddd.services.AccountModule
import recipes.ScalazRecipes.RecipesDaemons

import scalaz._
import Scalaz._
import java.util.Date
import scalaz.Kleisli._
import scalaz.concurrent.Task

trait AccountModuleImpl extends AccountModule[Task, Account, Amount, Balance] {
  mixin: { def executor: java.util.concurrent.ExecutorService } ⇒

  private trait DC

  private case object D extends DC

  private case object C extends DC

  override def open(no: String, name: String, rate: Option[BigDecimal], openingDate: Option[Date],
                    accountType: AccountType): AccountOperation[Account] =
    kleisli[Task, AccountRepo, ddd.Valid[Account]] { repo: AccountRepo ⇒
      Task {
        repo.query(no)
          .fold({ error ⇒ error.toString().failureNel[Account] }, { account: Option[Account] ⇒
            account.fold(accountType match {
              case Checking ⇒ Account.checkingAccount(no, name, openingDate, None, Balance()).flatMap(repo.store)
              case Savings ⇒ rate map { r ⇒
                Account.savingsAccount(no, name, r, openingDate, None, Balance()).flatMap(repo.store)
              } getOrElse s"Rate needs to be given for savings account".failureNel[Account]
            }) { r ⇒
              s"Already existing account with no $no".failureNel[Account]
            }
          })
      }(executor)
    }

  /**
   *
   */
  override def debit(no: String, amount: Amount): AccountOperation[Account] =
    modify(no, amount, D)

  /**
   *
   */
  override def credit(no: String, amount: Amount): AccountOperation[Account] =
    modify(no, amount, C)

  /**
   *
   */
  private def modify(no: String, amount: Amount, dc: DC): AccountOperation[Account] =
    kleisli[Task, AccountRepo, ddd.Valid[Account]] { (repo: AccountRepo) ⇒
      Task {
        repo.query(no).fold(
          { error ⇒ error.toString().failureNel[Account] }, { a: Option[Account] ⇒
            a.fold(s"Account $no does not exist".failureNel[Account]) { a ⇒
              dc match {
                case D ⇒ Account.updateBalance(a, -amount).flatMap(repo.store)
                case C ⇒ Account.updateBalance(a, amount).flatMap(repo.store)
              }
            }
          })
      }(executor)
    }

  override def balance(no: String): AccountOperation[Balance] =
    kleisli[Task, AccountRepo, ddd.Valid[Balance]] { (repo: AccountRepo) ⇒ Task(repo.balance(no)) }

  override def transfer(accounts: ddd.Valid[(String, String)], amount: Amount): AccountOperation[(Account, Account)] = {
    accounts.fold({ ex ⇒
      Kleisli.kleisli[Task, AccountRepo, ddd.Valid[(Account, Account)]] { r: AccountRepo ⇒
        Task.delay(Failure(ex))
      }
    }, { fromTo: (String, String) ⇒
      for {
        a ← debit(fromTo._1, amount)
        b ← credit(fromTo._2, amount)
      } yield (a |@| b) { case (a, b) ⇒ println("action [transfer]: " + Thread.currentThread().getName); (a, b) }
    })
  }

  /**
   *
   *
   */
  override def close(no: String, closeDate: Option[Date]): AccountOperation[Account] =
    kleisli[Task, AccountRepo, ddd.Valid[Account]] { repo: AccountRepo ⇒
      Task {
        repo.query(no).fold(
          { error ⇒ error.toString().failureNel[Account] }, { a: Option[Account] ⇒
            a.fold(s"Account $no does not exist".failureNel[Account]) { a ⇒
              val cd = closeDate.getOrElse(ddd.account.today)
              Account.close(a, cd).flatMap(repo.store)
            }
          })
      }(executor)
    }
}

object AccountService extends AccountModuleImpl {
  val executor = java.util.concurrent.Executors.newFixedThreadPool(2, new RecipesDaemons("accounts"))
}