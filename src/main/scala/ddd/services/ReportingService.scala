package ddd.services

import ddd.repo.AccountRepo

import scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

trait ReportingService[Amount] {

  type ReportOperation[A] = Kleisli[Task, AccountRepo, ddd.Valid[A]]

  def balances: ReportOperation[Seq[(String, Amount)]]

  def balancesProcess: Reader[AccountRepo, Process[Task, ddd.Valid[Seq[(String, Amount)]]]]
}