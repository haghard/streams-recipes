package ddd.services

import ddd.repo.AccountRepo
import ddd.account.AccountType

import scalaz.Kleisli
import java.util.Date

trait AccountModule[M[_], Account, Amount, Balance] {

  type AccountOperation[A] = Kleisli[M, AccountRepo, ddd.Valid[A]]

  def open(
    no: String,
    name: String,
    rate: Option[BigDecimal],
    openingDate: Option[Date],
    accountType: AccountType
  ): AccountOperation[Account]

  def close(no: String, closeDate: Option[Date]): AccountOperation[Account]

  def debit(no: String, amount: Amount): AccountOperation[Account]

  def credit(no: String, amount: Amount): AccountOperation[Account]

  def balance(no: String): AccountOperation[Balance]

  def transfer(accounts: ddd.Valid[(String, String)], amount: Amount): AccountOperation[(Account, Account)]
}
