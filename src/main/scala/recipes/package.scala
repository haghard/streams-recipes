package object recipes {

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
