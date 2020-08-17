import cats.Monoid

package object recipes {

  //  https://softwaremill.com/beautiful-folds-in-scala/
  //  https://github.com/adamw/beautiful-folds.git

  case class Average[A](numerator: A, denominator: Int)

  implicit def averageMonoid[A: Numeric]: Monoid[Average[A]] =
    new Monoid[Average[A]] {
      override def empty = Average(implicitly[Numeric[A]].zero, 0)

      override def combine(l: Average[A], r: Average[A]) =
        Average(implicitly[Numeric[A]].plus(l.numerator, r.numerator), l.denominator + r.denominator)
    }

  object CustomMonoids {
    import Ordering.Implicits._

    case class Max[A](v: A)

    def maxMonoid[A: Ordering](minValue: A): Monoid[Max[A]] =
      new Monoid[Max[A]] {
        override val empty                                 = Max(minValue)
        override def combine(x: Max[A], y: Max[A]): Max[A] = if (x.v < y.v) y else x
      }
    //val maxIntMonoid = maxMonoid(Int.MinValue)

    def numProductMonoid[A: Numeric]: Monoid[A] =
      new Monoid[A] {
        override val empty                  = implicitly[Numeric[A]].one
        override def combine(l: A, r: A): A = implicitly[Numeric[A]].times(l, r)
      }

    def firstMonoid[T]: Monoid[Option[T]] =
      new Monoid[Option[T]] {
        override val empty                                          = None
        override def combine(l: Option[T], r: Option[T]): Option[T] = l.orElse(r)
      }

    def lastMonoid[T]: Monoid[Option[T]] =
      new Monoid[Option[T]] {
        override val empty                                          = None
        override def combine(l: Option[T], r: Option[T]): Option[T] = r.orElse(l)
      }

    def andMonoid: Monoid[Boolean] =
      new Monoid[Boolean] {
        override val empty                                    = true
        override def combine(x: Boolean, y: Boolean): Boolean = x && y
      }

    def orMonoid: Monoid[Boolean] =
      new Monoid[Boolean] {
        override def empty                                    = false
        override def combine(x: Boolean, y: Boolean): Boolean = x || y
      }
  }

  trait SourceElement[T] {
    def apply(v: T): Double
  }

  implicit val int2Double = new SourceElement[Int] {
    def apply(v: Int): Double = v.toDouble
  }

  implicit val long2Double = new SourceElement[Long] {
    def apply(v: Long): Double = v.doubleValue
  }

  implicit val double2Double = new SourceElement[Double] {
    def apply(v: Double): Double = v
  }
}
