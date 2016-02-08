import java.util.concurrent.ExecutorService

import scala.reflect.ClassTag
import recipes.ScalazRecipes.RecipesDaemons

package object cake {
  import scala.concurrent.Future
  import scalaz.concurrent.Task
  import scalaz._, Scalaz._

  import util.Try
  import scalaz.concurrent.{Task => ZTask}
  import scala.concurrent.{ExecutionContext, Future => SFuture, Promise}

  //Integration code between Scalaz and Scala standard concurrency libraries
  object Task2Future {

    def fromScala[A](future: SFuture[A])(implicit ec: ExecutionContext): ZTask[A] =
      scalaz.concurrent.Task.async(handlerConversion andThen future.onComplete)

    def fromScalaDeferred[A](future: => SFuture[A])(implicit ec: ExecutionContext): ZTask[A] =
      scalaz.concurrent.Task.delay(fromScala(future)(ec)).flatMap(identity)

    def unsafeToScala[A](task: ZTask[A]): SFuture[A] = {
      val p = Promise[A]
      task.runAsync { _ fold (p failure _, p success _) }
      p.future
    }

    private def handlerConversion[A]: ((Throwable \/ A) => Unit) => Try[A] => Unit =
      callback => { t: Try[A] => \/.fromTryCatchNonFatal(t.get) } andThen callback
  }

  trait ScalazParallelism[M[_]] {
    implicit def Executor: java.util.concurrent.ExecutorService
    def ND: scalaz.Nondeterminism[M]
  }

  //http://logji.blogspot.ru/2014/02/the-abstract-future.html
  trait TwitterModule[M[_]] { mixin: ScalazParallelism[M] =>
    type Tweet
    type TwitterApi <: TwitterApiLike
    type ValidTweet = ValidationNel[String, Tweet]

    protected trait TwitterApiLike {
      def load(query: String): M[ValidTweet]
      def reduce(query: String): M[ValidTweet]
      protected def take(n: Int): List[Tweet]
    }

    def twitterApi: TwitterApi
    def twitterApp: scalaz.Apply[M]
  }

  trait UserModule[M[_]] { mixin: ScalazParallelism[M] =>
    type Record
    type UserApi <: DbUserLike
    type ValidRecord = ValidationNel[String, Record]

    protected trait DbUserLike {
      def one(query: String): M[ValidRecord]
      def page(query: String): M[ValidRecord]
    }

    def dbApi: UserApi
    def dbApp: scalaz.Apply[M] //scalaz.Applicative[M] or Monad[M]
  }

  /*
  object TwitterModule {
    def apply[M[_]: scalaz.Monad] = new TwitterModule[M]{}
  }

  implicit def FutureMonad(implicit ctx: scala.concurrent.ExecutionContext) = new Monad[Future] {
    def point[A](a: => A): Future[A] = Future(a)(ctx)
    def bind[A, B](fa: Future[A])(f: (A) => Future[B]): Future[B] = fa flatMap f
  }
  */

  object Services {

    object Implicits {

      implicit def twitterTaskInt: Task[ValidationNel[String, Int]] = Task(0.successNel[String])

      implicit def twitterTaskStr: Task[ValidationNel[String, String]] = Task("0".successNel[String])

      implicit def twitterFutureInt: Future[ValidationNel[String, Int]] =
        Future(0.successNel[String])(scala.concurrent.ExecutionContext.Implicits.global)

      implicit def twitterFutureStr: Future[ValidationNel[String, String]] =
        Future("0".successNel[String])(scala.concurrent.ExecutionContext.Implicits.global)
    }

    def apply[T, M[_]](implicit effect: M[ValidationNel[String,T]], hoTag: ClassTag[M[_]], outT: ClassTag[T]): M[ValidationNel[String,T]] = {
      println(s"executable effect: ${hoTag.runtimeClass.getName}[${outT.runtimeClass.getName}]")
      effect
    }
  }

  trait ScalazFutureTwitter extends TwitterModule[scalaz.concurrent.Future] {
    mixin: ScalazParallelism[scalaz.concurrent.Future] =>
    override type Tweet = Int
    override type TwitterApi = ScalazFutureApi

    override lazy val twitterApp = scalaz.Apply[scalaz.concurrent.Future]

    //Monad
    //scalaz.Applicative[scalaz.concurrent.Future] = scalaz.Applicative[scalaz.concurrent.Future]

    final class ScalazFutureApi extends TwitterApiLike {

      override def load(query: String): scalaz.concurrent.Future[ValidTweet] =
        scalaz.concurrent.Future(0.successNel[String])

      override def reduce(query: String): scalaz.concurrent.Future[ValidTweet] = {
        scalaz.concurrent.Future {
          Thread.sleep(200)
          println(s"reduce:start ${Thread.currentThread().getName}")
          Thread.sleep(800)
          println(s"reduce:end ${Thread.currentThread().getName}")
          take(5).foldMap(i => i)(implicitly[Monoid[Tweet]]).successNel[String]
        }
      }

      protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazFutureApi()
  }

  trait ScalazTaskTwitter extends TwitterModule[Task] { mixin: ScalazParallelism[Task] =>
    override type Tweet = Int
    override type TwitterApi = TwitterApiLike

    override lazy val twitterApp = scalaz.Apply[Task] //scalaz.Applicative[Task]

    final class ScalazTaskApi extends TwitterApiLike {

      override def load(query: String): Task[ValidTweet] = Task(0.successNel[String])(Executor)

      override def reduce(query: String): Task[ValidTweet] =
        Task {
          println(s"reduce:start ${Thread.currentThread().getName}")
          Thread.sleep(400)
          println(s"reduce:stop ${Thread.currentThread().getName}")
          (1.successNel[String] :: 2.successNel[String] :: /*"3 error".failureNel[Tweet] :: "4 error".failureNel[Tweet] ::*/ Nil).sequenceU
          .map(_.foldMap(identity)(implicitly[Monoid[Tweet]]))
        }(Executor)

      override protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazTaskApi()
  }

  trait ScalazFutureDbService extends UserModule[scalaz.concurrent.Future] { mixin: ScalazParallelism[scalaz.concurrent.Future] =>
    override type Record = Int
    override type UserApi = ScalazFutureApi

    override lazy val dbApp = scalaz.Apply[scalaz.concurrent.Future]

    //scalaz.Applicative[scalaz.concurrent.Future] = scalaz.Applicative[scalaz.concurrent.Future]

    final class ScalazFutureApi extends DbUserLike {
      override def one(query: String) =
        scalaz.concurrent.Future {
          Thread.sleep(200)
          println(s"one:start ${Thread.currentThread().getName}")
          Thread.sleep(800)
          println(s"one:end ${Thread.currentThread().getName}")
          //throw new Exception("one error")
          0.successNel[String]
        }

      override def page(query: String) =
        scalaz.concurrent.Future {
          //Thread.sleep(500)
          println(s"page:start ${Thread.currentThread().getName}")
          //Thread.sleep(300)
          println(s"page:end ${Thread.currentThread().getName}")
          //throw new Exception("page error")
          (1.successNel[String] :: 2.successNel[String] :: /*"3 error".failureNel[Record] :: "4 error".failureNel[Record] ::*/ Nil).sequenceU
            .map(_.foldMap(i=>i)(implicitly[Monoid[Record]]))
        }
    }

    override lazy val dbApi = new ScalazFutureApi()
  }

  trait MySqlTaskDbService extends UserModule[Task] with ScalazTaskTwitter { mixin: ScalazParallelism[Task] =>
    override type Record = Int
    override type UserApi = MySqlApi

    override lazy val dbApp = scalaz.Apply[Task]

    final class MySqlApi extends DbUserLike {
      override def one(query: String) = Task { 1.successNel[String] }(Executor)

      override def page(query: String): Task[ValidRecord] =
        Task {
          println(s"page:start ${Thread.currentThread().getName}")
          Thread.sleep(500l)
          println(s"page:stop ${Thread.currentThread().getName}")
          (10.successNel[String] :: 11.successNel[String] :: /*"12 error".failureNel[Record] :: "13 error".failureNel[Record] ::*/ Nil).sequenceU
            .map(_.foldMap(i => i)(implicitly[Monoid[Record]]))
        }(Executor)
    }

    override lazy val dbApi = new MySqlApi()
  }

  object KleisliSupport {
    import scalaz.Kleisli

    type Reader[T] = scalaz.ReaderT[Task, ExecutorService, T]
    type Delegated[A] = Kleisli[Task, ExecutorService, A]

    def delegate: Delegated[ExecutorService] = Kleisli.kleisli(e ⇒ Task.now(e))
    def reader: Reader[ExecutorService] = Kleisli.kleisli(e ⇒ Task.now(e))

    implicit class KleisliTask[T](val task: Task[T]) extends AnyVal {
      def kleisli: Delegated[T] = Kleisli.kleisli(_ ⇒ task)
      def kleisliR: Reader[T] = Kleisli.kleisli(_ ⇒ task)
    }
  }

  //http://blog.scalac.io/2015/10/15/shapeless-and-futures.html
  trait ShapelessMonadSupport {
    import shapeless._
    import shapeless.ops.function.FnToProduct
    import shapeless.ops.hlist.Tupler
    import scala.languageFeature.implicitConversions
    import scalaz.Monad
    import scalaz.syntax.ToMonadOps

    trait HListOfMonad[M[_], In <: HList, Out <: HList] extends ToMonadOps {
      def parallelHList(l: In)(implicit nd: scalaz.Nondeterminism[M]): M[Out]
      def sequenceHList(l: In): M[Out]
    }

    object HListOfMonad {
      def apply[M[_], In <: HList, Out <: HList](implicit isHM: HListOfMonad[M, In, Out], m: Monad[M]): HListOfMonad[M, In, Out] = isHM

      implicit def IsHNilHListOfM[M[_]](implicit m: Monad[M]) = new HListOfMonad[M, HNil, HNil] {
        override def parallelHList(l: HNil)(implicit nd: scalaz.Nondeterminism[M]): M[HNil] = m.pure(HNil)
        override def sequenceHList(l: HNil): M[HNil] = m.pure(HNil)
      }

      implicit def hconsIsHListOfM[M[_], H, In <: HList, Out <: HList](implicit ev: HListOfMonad[M, In, Out], m: Monad[M]):
        HListOfMonad[M, M[H] :: In, H :: Out] = new HListOfMonad[M, M[H] :: In, H :: Out] {
          //concurrent
          override def parallelHList(list: M[H] :: In)(implicit nd: scalaz.Nondeterminism[M]): M[H :: Out] =
            nd.mapBoth(list.head, ev.parallelHList(list.tail))(_ :: _)

          //sequentual
          override def sequenceHList(list: M[H] :: In): M[H :: Out] =
            list.head.flatMap(h => ev.sequenceHList(list.tail).map(h :: _))
        }
    }

    def zip[M[_], P <: Product, In <: HList, Out <: HList](p: P)(implicit gen: Generic.Aux[P, In], ev: HListOfMonad[M, In, Out], tupler: Tupler[Out],
                                                                 m: Monad[M], nd: scalaz.Nondeterminism[M]) =
      m.map(parallelHList(gen to p))(_.tupled)

    def parallelHList[M[_], In <: HList, Out <: HList](l: In)(implicit M: HListOfMonad[M, In, Out], m: Monad[M], nd: scalaz.Nondeterminism[M]): M[Out] =
      M.parallelHList(l)

    def sequenceHList[M[_], In <: HList, Out <: HList](l: In)(implicit M: HListOfMonad[M, In, Out], m: Monad[M]): M[Out] =
      M.sequenceHList(l)

    case class NondeterministicHListApplicativeBuilder[M[_], In <: HList, Out <: HList](values: In)(implicit m: Monad[M]) {
      def asTuple[T](implicit ev: HListOfMonad[M, In, Out], m: Monad[M], tupler: Tupler.Aux[Out, T], nd: scalaz.Nondeterminism[M]): M[T] =
        m.map(parallelHList(values))(_.tupled)

      def apply[F, FOut](f: F)(implicit fnEv: FnToProduct.Aux[F, Out => FOut], ev: HListOfMonad[M, In, Out], nd: scalaz.Nondeterminism[M]): M[FOut] =
        m.map(parallelHList(values))(fnEv(f))

      /**
        *
        *
        */
      def ||@||[X, T1](next: M[X]) = NondeterministicHListApplicativeBuilder[M, M[X] :: In, X :: Out](next :: values)
    }

    implicit def ToApplicativeBuilder[M[_], V](value: M[V])(implicit ev: HListOfMonad[M, M[V] :: HNil, V :: HNil], m: Monad[M]): NondeterministicHListApplicativeBuilder[M, M[V] :: HNil, V::HNil] =
      new NondeterministicHListApplicativeBuilder[M, M[V] :: HNil, V :: HNil](value :: HNil)
  }

  def monoidPar[T: Monoid, M[_]: Monad : Nondeterminism]: Monoid[M[T]] = new Monoid[M[T]] {
    val m = implicitly[Monoid[T]]
    val M = implicitly[Monad[M]]
    val ND = implicitly[Nondeterminism[M]]

    override val zero = M.pure(m.zero)

    override def append(a: M[T], b: ⇒ M[T]): M[T] =
      ND.nmap2(a, b) { (l, r) ⇒
        val res = m.append(l, r)
        println(s"${Thread.currentThread().getName}: $l and $r = $res")
        res
      }
  }

  def validate[F[_]: Traverse, A, B](fa : F[A])(map : A => B): ValidationNel[Throwable, F[B]] = {
    Applicative[({type l[a] = ValidationNel[Throwable, a]})#l]
      .traverse(fa)(a => Validation.fromTryCatchNonFatal[B](map(a)).toValidationNel)
  }

  val program = new ScalazTaskTwitter with MySqlTaskDbService with ScalazParallelism[Task] {
    override implicit lazy val Executor: java.util.concurrent.ExecutorService =
      java.util.concurrent.Executors.newFixedThreadPool(3, new RecipesDaemons("tasks"))

    override def ND = scalaz.Nondeterminism[Task]
  }


  object ProgramWithFuture extends ScalazFutureTwitter with ScalazFutureDbService
    with ScalazParallelism[scalaz.concurrent.Future] {

    override implicit lazy val Executor = java.util.concurrent.Executors.newFixedThreadPool(3, new RecipesDaemons("futures"))

    override def ND = scalaz.Nondeterminism[scalaz.concurrent.Future]
  }

  object ShapelessProgram extends ScalazTaskTwitter
    with MySqlTaskDbService with ScalazParallelism[Task]
    with ShapelessMonadSupport {

    override implicit lazy val Executor: java.util.concurrent.ExecutorService =
      java.util.concurrent.Executors.newFixedThreadPool(3, new RecipesDaemons("tasks0"))

    override lazy val ND = scalaz.Nondeterminism[scalaz.concurrent.Task]

    /**
      * Concurrent execution with ApplicativeBuilder and Shapeless
      * It allows doing the same things as original Applicative Builder but this is not limited to 12 elements.
      */
    def gather =
      ((twitterApi reduce "reduce page") ||@|| (dbApi page "select page") ||@|| (dbApi page "select page")) { (a: ValidTweet, b: ValidRecord, c: ValidRecord) ⇒
        s"[twitter:$a] - [db1:$b] - [db2:$c]" }

    /**
      *
      *
      */
    def gatherPHList = {
      val tasks = (twitterApi reduce "reduce page") :: (dbApi page "select page") :: (twitterApi reduce "reduce page") :: shapeless.HNil
      parallelHList(tasks).map { tupler: shapeless.::[ValidTweet,shapeless.::[ValidRecord,shapeless.::[ValidTweet,shapeless.HNil]]] =>
        (tupler.head |@| tupler.tail.head |@| tupler.tail.tail.head) { case (a, b, c) => s"A:$a - B:$b - C:$c" }
      }
    }

    /**
      *
      *
      */
    def gatherZip =
      zip(
        (twitterApi reduce "reduce page"),
        (dbApi page "select page"),
        (twitterApi reduce "reduce page")
      ).map { tupler: (ValidTweet, ValidRecord, ValidTweet) => (tupler._1 |@| tupler._2 |@| tupler._3) { case (a, b, c) => s"A:$a B:$b C:$c" } }

    /**
      * Sequentual with ApplicativeBuilder and Shapeless
      */
    def gatherSHList = {
      val tasks = (twitterApi reduce "reduce page") :: (dbApi page "select page") :: (twitterApi reduce "reduce page") :: shapeless.HNil
      sequenceHList(tasks)
    }
  }


  object ProgramWithTask extends ScalazTaskTwitter with MySqlTaskDbService
    with ScalazParallelism[Task] {
    import KleisliSupport._

    override implicit lazy val Executor: java.util.concurrent.ExecutorService =
      java.util.concurrent.Executors.newFixedThreadPool(3, new RecipesDaemons("tasks1"))

    override lazy val ND = scalaz.Nondeterminism[scalaz.concurrent.Task]

    /**
      * Concurrent
      */
    def gather0 =
      (for {
        ex ← reader
        results <- ND.both((twitterApi reduce "reduce page"), (dbApi page "select page")).kleisliR
        out = (results._1 |@| results._2) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
      } yield out).run(Executor)

    /**
      * Concurrent
      */
    def gatherP1 =
      ND.gatherUnordered(Seq(twitterApi.reduce("reduce page"), dbApi.page("select page"))).map(_.sequenceU)

    /**
      * Concurrent
      */
    def gatherP2 = ND.mapBoth(twitterApi.reduce("reduce page"), dbApi.page("select page")) { (x, y) =>
      (x |@| y) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
    }

    /*
      ND.nmap2(twitterApi.reduce("reduce page"), dbApi.page("select page")) { (x, y) =>
        (x |@| y) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
      }
    */

    /**
      * Sequentual
      * Imposes a total order on the sequencing of effects throughout a computation
      */
    def gatherS1 =
      twitterApp.apply2(
        twitterApi.reduce("select page"),
        dbApi.page("select page")
      ) { (x, y) => ((x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"}) }

    /**
      * Sequentual
      */
    def gatherS2 = for {
      x <- twitterApi.reduce("select page")
      y <- dbApi.page("select page")
    } yield ((x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"})


    /**
      * Sequentual
      */
    def gatherS3 =
      dbApp.apply2(
        (twitterApi reduce "select page"),
        (dbApi page "select page")
      ) { (x, y) => ((x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"}) }

    /**
      * Sequentual
      */
    def gatherS4 =
      twitterApp.ap2(
        twitterApi.reduce("select page"),
        dbApi.page("select page")
      )(Task { (x: ValidTweet, y: ValidRecord) => (x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"} }(Executor))

    /**
      * Sequentual with ApplicativeBuilder
      */
    def gatherS5 = ((twitterApi reduce "reduce page") |@| (dbApi page "select page")) { (x, y) =>
      (x |@| y) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
    }

    // Sequentual with
    //def gatherS6 = dbApp.sequence(List((twitterApi reduce "select page"), (dbApi page "select page")))
  }

/*
  import services._
  import scalaz._, Scalaz._

  type Out = FutureService.UserResult[FutureService.R]
  val api = FutureService.userApi

  val c = new java.util.concurrent.CountDownLatch(1)

  (api.page("select page") zip api.one("select one"))
    .map(r ⇒ (r._1 |@| r._2) { case (a, b) ⇒ s"A:$a B:$b" })
    .onComplete(_.map { r ⇒ println(r.shows); c.countDown() })

  scalaz.Applicative[scala.concurrent.Future].ap2(api.page("select page"), api.one("select one")) {
    scala.concurrent.Future { (l: Out, r: Out) ⇒ (l |@| r) { case (a, b) ⇒ s"A:$a B:$b" } }
  }.onComplete { _.map { r ⇒ println(r.shows); c.countDown() } }

  scalaz.Applicative[scala.concurrent.Future].apply2(api.page("select page"), api.one("")) { (l, r) ⇒
    (l |@| r) { case (a, b) ⇒ s"A:$a B:$b" }
  }.onComplete { _.map { r ⇒ println(r.shows); c.countDown() } }

  cats.Applicative[scala.concurrent.Future].ap2(api.page("select page"), api.one("select one")) {
    scala.concurrent.Future { (l: Out, r: Out) ⇒ (l |@| r) { case (a, b) ⇒ s"A:$a B:$b" } }
  }.onComplete { _.map { r ⇒ println(r.shows); c.countDown() } }

  cats.Applicative[scala.concurrent.Future].map2(api.page("select page"), api.one("select one")) { (page, one) ⇒
    (page|@|one) { case (a, b) ⇒ s"A:$a B:$b" }
  }.onComplete { _.map { r ⇒ println(r.shows); c.countDown() } }


  new ScalazUserTaskService{}.userApi.page("select page")
    .runAsync(_.map { r ⇒ println(r.shows); c.countDown() })

  import services.Services.Implicits._
  services.Services[Int, Task].run
  services.Services[Int, scala.concurrent.Future].value

  services.ApplicationFutureService.zip3
    .onComplete(_.map { r ⇒ println(r); c.countDown() })(services.ApplicationFutureService.TwitterCtx)

  c.await()
*/
}