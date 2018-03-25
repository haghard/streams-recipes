package object ddd {

  import java.util.{ Calendar, Date }

  import scalaz._
  import Scalaz._

  type Amount = BigDecimal

  case class Balance(amount: Amount = 0)

  type Valid[A] = ValidationNel[String, A]

  object account {

    sealed trait AccountType

    case object Checking extends AccountType

    case object Savings extends AccountType

    def today = Calendar.getInstance.getTime

    case class Address(
      no:     String,
      street: String,
      city:   String,
      state:  String,
      zip:    String)

    final case class CheckingAccount(
      no:          String,
      name:        String,
      dateOfOpen:  Option[Date],
      dateOfClose: Option[Date] = None,
      balance:     Balance      = Balance())
      extends Account

    final case class SavingsAccount(
      no:             String,
      name:           String,
      rateOfInterest: Amount,
      dateOfOpen:     Option[Date],
      dateOfClose:    Option[Date] = None,
      balance:        Balance      = Balance())
      extends Account

    sealed trait Account {
      def no: String

      def name: String

      def dateOfOpen: Option[Date]

      def dateOfClose: Option[Date]

      def balance: Balance
    }

    object Account {

      private def validateAccountNo(no: String): ValidationNel[String, String] =
        if (no.isEmpty || no.size < 5)
          s"Account No has to be at least 5 characters long: found $no".failureNel[String]
        else no.successNel[String]

      private def validateOpenCloseDate(od: Date, cd: Option[Date]): ValidationNel[String, (Option[Date], Option[Date])] =
        cd.map { c ⇒
          if (c before od)
            s"Close date [$c] cannot be earlier than open date [$od]"
              .failureNel[(Option[Date], Option[Date])]
          else (od.some, cd).successNel[String]
        }.getOrElse((od.some, cd).successNel[String])

      private def validateRate(rate: BigDecimal) =
        if (rate <= BigDecimal(0))
          s"Interest rate $rate must be > 0".failureNel[BigDecimal]
        else rate.successNel[String]

      def checkingAccount(
        no:        String,
        name:      String,
        openDate:  Option[Date],
        closeDate: Option[Date],
        balance:   Balance): ValidationNel[String, Account] = {
        (validateAccountNo(no) |@| validateOpenCloseDate(
          openDate.getOrElse(today),
          closeDate)) {
            case (n, d) ⇒ CheckingAccount(n, name, d._1, d._2, balance)
          }
      }

      def savingsAccount(
        no:        String,
        name:      String,
        rate:      BigDecimal,
        openDate:  Option[Date],
        closeDate: Option[Date],
        balance:   Balance): ValidationNel[String, Account] = {
        (validateAccountNo(no) |@| validateOpenCloseDate(
          openDate.getOrElse(today),
          closeDate) |@| validateRate(rate)) { (n, d, r) ⇒
            SavingsAccount(n, name, r, d._1, d._2, balance)
          }
      }

      private def validateAccountAlreadyClosed(a: Account) = {
        if (a.dateOfClose isDefined)
          s"Account ${a.no} is already closed".failureNel[Account]
        else a.success
      }

      private def validateCloseDate(a: Account, cd: Date) = {
        if (cd before a.dateOfOpen.get)
          s"Close date [$cd] cannot be earlier than open date [${a.dateOfOpen.get}]"
            .failureNel[Date]
        else cd.success
      }

      def close(a: Account, closeDate: Date): ValidationNel[String, Account] = {
        (validateAccountAlreadyClosed(a) |@| validateCloseDate(a, closeDate)) {
          (acc, d) ⇒
            acc match {
              case c: CheckingAccount ⇒ c.copy(dateOfClose = Some(closeDate))
              case s: SavingsAccount  ⇒ s.copy(dateOfClose = Some(closeDate))
            }
        }
      }

      private def checkBalance(a: Account, amount: Amount) = {
        if (amount < 0 && a.balance.amount < -amount)
          s"Insufficient amount in ${a.no} to debit".failureNel[Account]
        else a.success
      }

      def updateBalance(
        a:      Account,
        amount: Amount): ValidationNel[String, Account] = {
        (validateAccountAlreadyClosed(a) |@| checkBalance(a, amount)) {
          (_, _) ⇒
            a match {
              case c: CheckingAccount ⇒
                c.copy(balance = Balance(c.balance.amount + amount))
              case s: SavingsAccount ⇒
                s.copy(balance = Balance(s.balance.amount + amount))
            }
        }
      }

      def rate(a: Account) = a match {
        case SavingsAccount(_, _, r, _, _, _) ⇒ r.some
        case _                                ⇒ None
      }
    }
  }
}
