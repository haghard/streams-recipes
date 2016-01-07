package ddd.services

import ddd.repo.AccountRepo
import ddd.account.AccountType
import recipes.ScalazRecipes.RecipesDaemons

import scalaz.Kleisli
import java.util.Date

import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait AccountService[Account, Amount, Balance] {

  type AccountOperation[A] = Kleisli[Task, AccountRepo, ddd.Valid[A]]

  //override def ND = scalaz.Nondeterminism[scalaz.concurrent.Task]

  def open(no: String, name: String, rate: Option[BigDecimal], openingDate: Option[Date], accountType: AccountType): AccountOperation[Account]

  def close(no: String, closeDate: Option[Date]): AccountOperation[Account]

  def debit(no: String, amount: Amount): AccountOperation[Account]

  def credit(no: String, amount: Amount): AccountOperation[Account]

  def balance(no: String): AccountOperation[Balance]

  /**
   *
   *
   */
  def transfer(accounts: ddd.Valid[(String, String)], amount: Amount): AccountOperation[(Account, Account)] = {
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
}