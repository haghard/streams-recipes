package ddd.services

import ddd.repo.AccountRepo
import scalaz._

trait ReportingModule[M[_]] {
  type T
  type ReportOperation[A] = Kleisli[M, AccountRepo, ddd.Valid[A]]

  def balances: ReportOperation[Seq[T]]
}
