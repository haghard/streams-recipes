import java.util.concurrent.ExecutorService

import recipes.ScalazRecipes.StreamThreadFactory

import scala.reflect.ClassTag

package object services {
  import scala.concurrent.Future
  import scalaz.concurrent.Task
  import scalaz._, Scalaz._

  trait Parallelism[M[_]] {
    implicit def Executor: java.util.concurrent.ExecutorService
    def ND: scalaz.Nondeterminism[M]
  }

  trait TwitterModule[M[_]] {
    type Tweet
    type TwitterApi <: TwitterApiLike
    type ValidTweet = ValidationNel[String, Tweet]

    def monadicTwitterContext: scalaz.Monad[M]

    protected trait TwitterApiLike {
      def load(query: String): M[ValidTweet]
      def reduce(query: String): M[ValidTweet]
      protected def take(n: Int): List[Tweet]
    }

    def twitterApi: TwitterApi
  }

  trait DbServiceModule[M[_]] {

    type Record
    type DbApi <: DbApiLike
    type ValidRecord = ValidationNel[String, Record]

    def monadicDbContext: scalaz.Monad[M]

    protected trait DbApiLike {
      def one(query: String): M[ValidRecord]
      def page(query: String): M[ValidRecord]
    }

    def dbApi: DbApi
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

      implicit def twitterTaskInt: Task[ValidationNel[String, Int]] =
        Task(0.successNel[String])

      implicit def twitterTaskStr: Task[ValidationNel[String, String]] =
        Task("0".successNel[String])

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
    mixin: Parallelism[scalaz.concurrent.Future] =>

    override type Tweet = Int
    override type TwitterApi = ScalazFutureApi
    override lazy val monadicTwitterContext: scalaz.Monad[scalaz.concurrent.Future] = scalaz.Monad[scalaz.concurrent.Future]

    final class ScalazFutureApi extends TwitterApiLike {

      override def load(query: String): scalaz.concurrent.Future[ValidTweet] =
        scalaz.concurrent.Future(0.successNel[String])

      override def reduce(query: String): scalaz.concurrent.Future[ValidTweet] = {
        scalaz.concurrent.Future {
          //Thread.sleep(200)
          println(s"reduce:start ${Thread.currentThread().getName}")
          //Thread.sleep(800)
          println(s"reduce:end ${Thread.currentThread().getName}")
          take(5).foldMap(i => i)(implicitly[Monoid[Tweet]]).successNel[String]
        }
      }

      protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazFutureApi()
  }

  trait ScalazTaskTwitter extends TwitterModule[Task] {
    mixin: Parallelism[Task] =>

    override type Tweet = Int
    override type TwitterApi = TwitterApiLike
    override lazy val monadicTwitterContext: scalaz.Monad[Task] = scalaz.Monad[Task]

    final class ScalazTaskApi extends TwitterApiLike {

      override def load(query: String): Task[ValidTweet] = Task(0.successNel[String])

      override def reduce(query: String): Task[ValidTweet] =
        Task {
          println(s"reduce:start ${Thread.currentThread().getName}")
          Thread.sleep(500)
          println(s"reduce:stop ${Thread.currentThread().getName}")
          (1.successNel[String] :: 2.successNel[String] /*:: "3 error".failureNel[Tweet] :: "4 error".failureNel[Tweet]*/ :: Nil).sequenceU
          .map(_.foldMap(t=>t)(implicitly[Monoid[Tweet]]))
        }(Executor)

      override protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazTaskApi()
  }

  trait ScalazFutureDbService extends DbServiceModule[scalaz.concurrent.Future] {
    mixin: Parallelism[scalaz.concurrent.Future] =>

    override type Record = Int
    override type DbApi = ScalazFutureApi

    override lazy val monadicDbContext: scalaz.Monad[scalaz.concurrent.Future] = scalaz.Monad[scalaz.concurrent.Future]

    final class ScalazFutureApi extends DbApiLike {
      override def one(query: String) =
        scalaz.concurrent.Future {
          Thread.sleep(200)
          println(s"one:start ${Thread.currentThread().getName}")
          Thread.sleep(800)
          println(s"one:end ${Thread.currentThread().getName}")
          //throw new Exception("one error")
          0.successNel[String]
        } //.recover { case ex: Throwable => ex.getMessage.failureNel[Record]}

      override def page(query: String) =
        scalaz.concurrent.Future {
          //Thread.sleep(500)
          println(s"page:start ${Thread.currentThread().getName}")
          //Thread.sleep(300)
          println(s"page:end ${Thread.currentThread().getName}")
          //throw new Exception("page error")
          (1.successNel[String] :: 2.successNel[String] :: /*"3 error".failureNel[Record] :: "4 error".failureNel[Record] ::*/ Nil).sequenceU
            .map(_.foldMap(i=>i)(implicitly[Monoid[Record]]))
        } //.recover { case ex: Throwable => ex.getMessage.failureNel[Record] }
    }

    override lazy val dbApi = new ScalazFutureApi()
  }

  trait ScalazTaskDbService extends DbServiceModule[Task] {
    mixin: Parallelism[Task] =>

    override type Record = Int
    override type DbApi = ScalazTaskApi

    override lazy val monadicDbContext: scalaz.Monad[Task] = scalaz.Monad[Task]

    final class ScalazTaskApi extends DbApiLike {
      override def one(query: String) =
        Task(0).attemptRun match {
          case \/-(r) => Task.now(r.successNel[String])
          case -\/(ex) => Task.now(ex.getMessage.failureNel[Record])
        }

      override def page(query: String): Task[ValidRecord] = {
        Task {
          println(s"page:start ${Thread.currentThread().getName}")
          Thread.sleep(800l)
          println(s"page:stop ${Thread.currentThread().getName}")
          (10.successNel[String] :: 11.successNel[String] :: /*"12 error".failureNel[Record] :: "13 error".failureNel[Record] ::*/ Nil).sequenceU
            .map(_.foldMap(i => i)(implicitly[Monoid[Record]]))
        }(Executor)
      }
    }

    override lazy val dbApi = new ScalazTaskApi()
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

  //object FutureDbService extends ScalazFutureDbService
  //object ScalazFutureTwitterService extends ScalazFutureTwitter

  //object TaskDbService extends ScalazTaskDbService
  //object ScalazTaskTwitterService extends ScalazTaskTwitter


  object ApplicationFutureService extends ScalazFutureTwitter with ScalazFutureDbService
    with Parallelism[scalaz.concurrent.Future] {

    override implicit lazy val Executor = java.util.concurrent.Executors.newFixedThreadPool(3, new StreamThreadFactory("futures"))

    override def ND = scalaz.Nondeterminism[scalaz.concurrent.Future]

    def gatherP =
      ND.gatherUnordered(Seq((twitterApi reduce "reduce page"), (dbApi page "select page")))
        .map(_.sequenceU)


    /**
      * Sequentual
      */
    def gatherS1 =
      monadicTwitterContext.apply2(
        (twitterApi reduce "select page"),
        (dbApi page "select page")
      ) { (a, b) => (a |@| b) { case (a, b) ⇒ s" ${Thread.currentThread().getName} - twitter:$a db:$b" } }

    /**
      * Sequentual
      */
    def gatherS2 = for {
      x <- twitterApi reduce "select page"
      y <- dbApi page "select page"
    } yield ((x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b" })
  }

  object ApplicationTaskService extends ScalazTaskTwitter with ScalazTaskDbService with Parallelism[Task] {
    import KleisliSupport._

    override implicit lazy val Executor = java.util.concurrent.Executors.newFixedThreadPool(3, new StreamThreadFactory("tasks"))

    override def ND = scalaz.Nondeterminism[scalaz.concurrent.Task]

    /**
      * Concurrent
      */
    def gatherP =
      (for {
        ex ← reader
        pair <- ND.both((twitterApi reduce "reduce page"), (dbApi page "select page")).kleisliR
        out = (pair._1 |@| pair._2) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
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
      monadicTwitterContext.apply2(
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
      monadicDbContext.apply2(
        (twitterApi reduce "select page"),
        (dbApi page "select page")
      ) { (x, y) => ((x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"}) }

    /**
      * Sequentual
      */
    def gatherS4 =
      monadicTwitterContext.ap2(
        twitterApi.reduce("select page"),
        dbApi.page("select page")
      )(Task { (x: ValidTweet, y: ValidRecord) => (x |@| y) { case (a, b) ⇒ s"twitter:$a db:$b"} }(Executor))

    /**
      * ApplicativeBuilder
      * @return
      */
    def gatherS5 = ((twitterApi reduce "reduce page") |@| (dbApi page "select page")) { (x, y) =>
      (x |@| y) { case (a, b) ⇒ s"${Thread.currentThread().getName} - twitter:$a db:$b" }
    }
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