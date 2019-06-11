import cats.Monoid

package object recipes {

  //  https://softwaremill.com/beautiful-folds-in-scala/
  //  https://github.com/adamw/beautiful-folds.git

  case class Average[A](numerator: A, denominator: Int)

  implicit def averageMonoid[A: Numeric] = new Monoid[Average[A]] {
    override def empty = Average(implicitly[Numeric[A]].zero, 0)

    override def combine(l: Average[A], r: Average[A]) = Average(
      implicitly[Numeric[A]].plus(l.numerator, r.numerator),
      l.denominator + r.denominator)
  }

  object CustomMonoids {
    import Ordering.Implicits._

    case class Max[A](v: A)

    def maxMonoid[A: Ordering](minValue: A): Monoid[Max[A]] = new Monoid[Max[A]] {
      override val empty = Max(minValue)
      override def combine(x: Max[A], y: Max[A]) = if (x.v < y.v) y else x
    }
    //val maxIntMonoid = maxMonoid(Int.MinValue)

    def numProductMonoid[A: Numeric] = new Monoid[A] {
      override val empty = implicitly[Numeric[A]].one
      override def combine(l: A, r: A) = implicitly[Numeric[A]].times(l, r)
    }

    def firstMonoid[T] = new Monoid[Option[T]] {
      override val empty = None
      override def combine(l: Option[T], r: Option[T]) = l.orElse(r)
    }

    def lastMonoid[T] = new Monoid[Option[T]] {
      override val empty = None
      override def combine(l: Option[T], r: Option[T]) = r.orElse(l)
    }

    def andMonoid = new Monoid[Boolean] {
      override val empty = true
      override def combine(x: Boolean, y: Boolean) = x && y
    }

    def orMonoid = new Monoid[Boolean] {
      override def empty = false
      override def combine(x: Boolean, y: Boolean) = x || y
    }
  }

  trait SourceType[T] {
    def apply(v: T): Double
  }

  implicit val int2Double = new SourceType[Int] {
    def apply(v: Int): Double = v.toDouble
  }

  implicit val long2Double = new SourceType[Long] {
    def apply(v: Long): Double = v.doubleValue
  }

  implicit val double2Double = new SourceType[Double] {
    def apply(v: Double): Double = v
  }
}
