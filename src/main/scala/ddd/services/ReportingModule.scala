package ddd.services

import ddd.repo.AccountRepo

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingModule[M[_]] {
  type T
  type ReportOperation[A] = Kleisli[M, AccountRepo, ddd.Valid[A]]

  def balances: ReportOperation[Seq[(String, T)]]

  def balancesProcess: Reader[AccountRepo, Process[Task, ddd.Valid[Seq[(String, T)]]]]
}