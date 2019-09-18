package dsl

import cats.Id
import cats.data.NonEmptyList
import cats.data.Validated
import cats.data.Validated._
import cats.syntax.semigroup._
import cats.syntax.validated._
import cats.syntax.apply._
import cats.data._
import cats.effect.IO

// import dsl.InvariantsDsl
//  Tagless final dsl for data validation problem
object InvariantsDsl {

  type Errors = NonEmptyList[String]
  type R[T]   = cats.data.Validated[Errors, T]

  /**
    * All operations supported by this dsl.
    *
    * @tparam F
    */
  trait PredicateOps[F[_]] {
    def inSet[T](in: T, state: Set[T], msg: String): F[R[T]]

    def notInSet[T](in: T, state: Set[T], msg: String): F[R[T]]

    def maybeInSet[T](in: Option[T], state: Set[T], msg: String): F[R[T]]

    def inMap[T](in: T, state: Map[T, _], msg: String): F[R[T]]

    def maybeInMap[T](in: Option[T], state: Map[T, _], msg: String): F[R[T]]

    def and[A, B](l: F[R[A]], r: F[R[B]]): F[R[List[Any]]] //F[R[(A,B)]]

    def or[A, B](l: F[R[A]], r: F[R[B]]): F[R[List[Any]]] //F[R[A Either B]]

    protected def toList[T](v: T): List[Any] = v match {
      case h :: t ⇒ h :: t
      case e      ⇒ List(e)
    }
  }

  trait DslElement[T] {
    def apply[F[_]](implicit F: PredicateOps[F]): F[T]
  }

  trait CheckProdDsl {

    def uniqueProd[T](in: T, state: Set[T]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.notInSet[T](in, state, "uniqueProductName")
    }

    def knownProd[T](in: T, state: Set[T]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.notInSet[T](in, state, "knownProductName")
    }

    def knownProd[T](in: T, state: Map[T, _]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.inMap[T](in, state, "knownProductMap")
    }

    def knownProd[T](in: Option[T], state: Map[T, _]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.maybeInMap[T](in, state, "knownProductOpt")
    }
  }

  trait CheckSpecDsl {

    def knownSpec[T](in: T, state: Set[T]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.inSet[T](in, state, "knownSpecSet")
    }

    def uniqueSpec[T](in: T, state: Set[T]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.notInSet[T](in, state, "uniqueSpec")
    }

    def knownSpec[T](in: T, state: Map[T, _]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.inMap[T](in, state, "knownSpecMap")
    }

    def knownSpec[T](in: Option[T], state: Map[T, _]): DslElement[R[T]] = new DslElement[R[T]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[T]] = C.maybeInMap[T](in, state, "knownSpecOptMap")
    }
  }

  trait BasicDsl { self ⇒

    def and[A, B](l: DslElement[R[A]], r: DslElement[R[B]]): DslElement[R[List[Any]]] = new DslElement[R[List[Any]]] {
      override def apply[F[_]](implicit C: PredicateOps[F]): F[R[List[Any]]] =
        C.and[A, B](l.apply[F], r.apply[F])
    }

    def or[A, B](l: DslElement[R[A]], r: DslElement[R[B]]): DslElement[R[List[Any]]] = new DslElement[R[List[Any]]] {
      override def apply[F[_]](implicit O: PredicateOps[F]): F[R[List[Any]]] =
        O.or[A, B](l.apply[F], r.apply[F])
    }

    implicit class DslOpts[A, B](dslL: DslElement[R[A]]) {
      def &&(dslR: DslElement[R[B]]): DslElement[R[List[Any]]] = self.and(dslL, dslR)

      def or(dslR: DslElement[R[B]]): DslElement[R[List[Any]]] = self.or(dslL, dslR)
    }

  }

  val catsIOops = new PredicateOps[cats.effect.IO] {

    override def inSet[T](in: T, state: Set[T], name: String): cats.effect.IO[R[T]] =
      cats.effect.IO {
        if (state.contains(in)) validNel(in)
        else invalidNel(s"$name failed")
      }

    override def notInSet[T](in: T, state: Set[T], name: String): IO[R[T]] =
      cats.effect.IO {
        if (state.contains(in)) invalidNel(s"$name failed")
        else validNel(in)
      }

    override def inMap[T](in: T, state: Map[T, _], name: String): IO[R[T]] =
      IO {
        state.get(in).fold[R[T]](invalidNel(s"$name failed")) { _ ⇒
          validNel(in)
        }
      }

    override def maybeInSet[T](in: Option[T], state: Set[T], name: String): IO[R[T]] =
      IO {
        in.fold[R[T]](validNel(in.asInstanceOf[T])) { el ⇒
          if (state.contains(el)) validNel(el)
          else invalidNel(s"$name failed")
        }
      }

    override def maybeInMap[T](in: Option[T], state: Map[T, _], name: String): IO[R[T]] =
      IO {
        in.fold[R[T]](validNel(in.asInstanceOf[T])) { el ⇒
          if (state.get(el).isDefined) validNel(el)
          else invalidNel(s"$name failed")
        }
      }

    override def and[A, B](l: IO[R[A]], r: IO[R[B]]): IO[R[List[Any]]] =
      for {
        a ← l
        b ← r
      } yield {
        println(s"DEBUG: $a AND $b")
        //throw new Exception("Booom !!!")
        a match {
          case Valid(left) ⇒
            b match {
              case Valid(right) ⇒
                Valid(toList[A](left) ::: toList[B](right))
              case Invalid(invR) ⇒ Invalid(invR)
            }
          case Invalid(left) ⇒
            b match {
              case Valid(_) ⇒
                Invalid(left)
              case Invalid(invR) ⇒
                Invalid(left ::: invR)
            }
        }
      }

    override def or[A, B](l: IO[R[A]], r: IO[R[B]]): IO[R[List[Any]]] =
      for {
        a ← l
        b ← r
      } yield {
        println(s"DEBUG: $a OR $b")
        a match {
          case Valid(left) ⇒
            b match {
              case Valid(right) ⇒
                Valid(toList[A](left) ::: toList[B](right))
              case Invalid(invR) ⇒
                Valid(toList[A](left))
            }
          case Invalid(left) ⇒
            b match {
              case Valid(right) ⇒
                Valid(toList[B](right))
              case Invalid(invR) ⇒
                Invalid(left ::: invR)
            }
        }
      }
  }

  val ops = new PredicateOps[cats.Id] {

    override def inSet[T](in: T, state: Set[T], name: String): Id[R[T]] =
      if (state.contains(in)) validNel(in)
      else invalidNel(s"$name failed")

    override def notInSet[T](in: T, state: Set[T], name: String): Id[R[T]] =
      if (!state.contains(in)) validNel(in)
      else invalidNel(s"$name failed")

    override def inMap[T](in: T, state: Map[T, _], name: String): Id[R[T]] =
      state.get(in).fold[R[T]](invalidNel(s"$name failed")) { _ ⇒
        validNel(in)
      }

    override def maybeInMap[T](in: Option[T], state: Map[T, _], name: String): Id[R[T]] =
      in.fold[R[T]](validNel(in.asInstanceOf[T]))(inMap(_, state, name))

    override def maybeInSet[T](in: Option[T], state: Set[T], name: String): R[T] =
      in.fold[R[T]](validNel(in.asInstanceOf[T]))(inSet(_, state, name))

    //HList instead of Any
    override def and[A, B](l: Id[R[A]], r: Id[R[B]]): /*Id[R[(A, B)]]*/ Id[R[List[Any]]] = {
      //Semigroupal.map2(l,r)((a, b) ⇒ (a, b))
      val (a, b) = (l, r).mapN { (a, b) ⇒
        (a, b)
      }
      println(s"DEBUG: $a AND $b")

      /*import cats.implicits._
        cats.Traverse[List].sequence(List(a, b))*/

      l match {
        case Valid(left) ⇒
          r match {
            case Valid(right) ⇒
              Valid(toList[A](left) ::: toList[B](right))
            case Invalid(invR) ⇒ Invalid(invR)
          }
        case Invalid(left) ⇒
          r match {
            case Valid(_) ⇒
              Invalid(left)
            case Invalid(invR) ⇒
              Invalid(left ::: invR)
          }
      }
    }

    override def or[A, B](l: Id[R[A]], r: Id[R[B]]): Id[R[List[Any]]] = {
      val (a, b) = (l, r).mapN { (a, b) ⇒
        (a, b)
      }
      println(s"DEBUG: $a OR $b")

      l match {
        case Valid(left) ⇒
          r match {
            case Valid(right) ⇒
              Valid(toList[A](left) ::: toList[B](right))
            case Invalid(invR) ⇒
              Valid(toList[A](left))
          }
        case Invalid(left) ⇒
          r match {
            case Valid(right) ⇒
              Valid(toList[B](right))
            case Invalid(invR) ⇒
              Invalid(left ::: invR)
          }
      }
    }
  }

  /*
  object Preconditions extends BasicDsl with CheckProdDsl with CheckSpecDsl

  import Preconditions._

  uniqueProd("a1", Set("a", "b", "c"))       //  Right("a1")
  uniqueProd("a1", Set("a", "b", "c", "a1")) // Left(a1 doesn't exist in the set)

  //without brackets
  or(
    and(
      uniqueSpec(1, Set(2, 3, 4, 6)),
      knownSpec(Some(21L), Map(21L → "a", 3L → "b"))
    ),
    uniqueProd("b", Set("b", "c"))
  )

  uniqueProd("b", Set("b", "c")) or knownSpec(Some(21L), Map(21L → "a", 3L → "b")) && uniqueSpec(1, Set(2, 3, 4, 6))

  //with brackets
  and(
    or(
      uniqueProd("b", Set("b", "c")),
      knownSpec(Some(21L), Map(21L → "a", 3L → "b"))
    ),
    uniqueSpec(1, Set(2, 3, 4, 6))
  )

  val expOr = (uniqueProd("b", Set("b", "c")) or knownSpec(Some(21L), Map(21L → "a", 3L → "b")))
    .&&(uniqueSpec(1, Set(2, 3, 4, 6)))

  expOr(ops)
   */
  //println("> " + expOr(interp))

}
