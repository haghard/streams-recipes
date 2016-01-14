package ddd

import java.util.Date

import ddd.repo.AccountRepo

import scalaz._
import Scalaz._
import Kleisli._

//runMain ddd.DDDRecipes
object DDDRecipes extends App {

  import ddd.account._
  import ddd.impl.AccountService._
  import ddd.impl.ReportingService._
  import ddd.impl.ReportingStreamingService. { balances => pBalances }

  val Account1 = "3445684569463567"
  val Account2 = "3463568456374573"

  def openBoth: Kleisli[scalaz.concurrent.Task, AccountRepo, ddd.Valid[(Balance, Balance)]] =
    for {
      a0 ← open(Account1, "Alan Turing", None, None, Checking)
      a1 ← open(Account2, "Nikola Tesla", BigDecimal(456.9).some, None, Savings)
    } yield (a0 |@| a1) { (a, b) ⇒ (a.balance, b.balance) }

  def creditBoth: Kleisli[scalaz.concurrent.Task, AccountRepo, ddd.Valid[(String, String)]] =
    for {
      a0 ← credit(Account1, 5500)
      a1 ← credit(Account2, 6000)
    } yield (a0 |@| a1) { (a, b) ⇒ (a.no, b.no) }

  trait AccountRepositoryInMemory extends AccountRepo {
    lazy val repo = scala.collection.concurrent.TrieMap.empty[String, Account]

    override def query(no: String): ValidationNel[String, Option[Account]] = {
      println(Thread.currentThread().getName + ": query")
      repo.get(no).success
    }

    override def store(a: Account): ValidationNel[String, Account] = {
      println(Thread.currentThread().getName + ": store")
      repo += (a.no -> a)
      a.success
    }

    override def query(openedOn: Date): ValidationNel[String, Seq[Account]] = {
      println(Thread.currentThread().getName + ": query")
      repo.values.filter(_.dateOfOpen == openedOn).toSeq.success
    }

    override def all: ValidationNel[String, Seq[Account]] = {
      println(Thread.currentThread().getName + ": all")
      repo.values.toSeq.success
    }
  }

  object AccountRepositoryFromMap extends AccountRepositoryInMemory

  val program = for {
    _ ← openBoth
    accounts ← creditBoth

    //balance1 ← balance(Account1)
    //balance2 <- balance(Account2)

    _ = accounts.foreach { vs: (String, String) ⇒ println(s"Accounts: [${vs._1} - ${vs._2}]") }

    _ ← transfer(accounts, BigDecimal(1000))

    c ← balances
  } yield c

  val r = program(AccountRepositoryFromMap).run
  r.foreach { seq ⇒
    seq.foreach { v: (String, Amount) ⇒
      println(s"${v._1}: ${v._2}")
    }
  }

  /*
  val Repo = AccountRepositoryFromMap
      val program = for { _ ← open2() } yield ()

      program(Repo)
      val res: Valid[Seq[(String, Amount)]] = balancesProcess.run(Repo).runLog.run(0)
      res.isSuccess === true
      res.toOption.get.size == 2
   */
}