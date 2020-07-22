package ddd.repo

import scalaz._
import Scalaz._

import ddd.Balance
import java.util.Date
import ddd.account.Account

trait AccountRepo {

  /**
    * @param no
    * @return
    */
  def query(no: String): ValidationNel[String, Option[Account]]

  /**
    * @param a
    * @return
    */
  def store(a: Account): ValidationNel[String, Account]

  /**
    * @param no
    * @return
    */
  def balance(no: String): ValidationNel[String, Balance] =
    query(no).fold(
      error ⇒ s"No account exists with no $no".failureNel[Balance],
      { a: Option[Account] ⇒
        a.fold("No account exists with no $no".failureNel[Balance]) { r ⇒
          r.balance.success
        }
      }
    )

  /**
    * @param openedOn
    * @return
    */
  def query(openedOn: Date): ValidationNel[String, Seq[Account]]

  /**
    * @return
    */
  def all: ValidationNel[String, Seq[Account]]
}
