import scala.reflect.ClassTag

package object services {
  import scala.concurrent.Future
  import scalaz.concurrent.Task
  import scalaz._, Scalaz._

  trait TwitterModule[M[_]] {
    type Tweet
    type TwitterContext
    type TwitterApi <: TwitterApiLike
    type ValidTweet = ValidationNel[String, Tweet]

    implicit def TwitterCtx: TwitterContext
    def twitterEffect: scalaz.Monad[M]

    protected trait TwitterApiLike {
      def load(query: String): M[ValidTweet]
      def reduce(query: String): M[ValidTweet]
      protected def take(n: Int): List[Tweet]
    }

    def twitterApi: TwitterApi
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

    def apply[T, M[_]](implicit effect: M[ValidationNel[String,T]], tag0: ClassTag[M[_]], tag1: ClassTag[T]): M[ValidationNel[String,T]] = {
      println(s"executable effect: ${tag0.runtimeClass.getName}[${tag1.runtimeClass.getName}]")
      effect
    }
  }

  trait DbServiceModule[M[_]] {
    type Record
    type DbContext
    type DbApi <: DbApiLike
    type ValidRecord = ValidationNel[String, Record]

    implicit def DbCtx: DbContext
    def dbEffect: scalaz.Monad[M]

    protected trait DbApiLike {
      def one(query: String): M[ValidRecord]
      def page(query: String): M[ValidRecord]
    }

    def dbApi: DbApi
  }

  trait ScalazFutureTwitter extends TwitterModule[Future] {
    override type Tweet = Int
    override type TwitterContext = scala.concurrent.ExecutionContext
    override type TwitterApi = ScalazFutureApi
    override val twitterEffect: scalaz.Monad[Future] = scalaz.Monad[Future]

    override implicit val TwitterCtx = scala.concurrent.ExecutionContext.Implicits.global

    final class ScalazFutureApi extends TwitterApiLike {

      override def load(query: String): Future[ValidTweet] =
        Future(0.successNel[String])(TwitterCtx)

      override def reduce(query: String): Future[ValidTweet] =
        Future(take(5).foldMap(i => i)(implicitly[Monoid[Tweet]]).successNel[String])(TwitterCtx)

      protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazFutureApi()
  }

  trait ScalazTaskTwitter extends TwitterModule[Task] {
    override type Tweet = Int
    override type TwitterContext = java.util.concurrent.Executor //scalaz.concurrent.Strategy
    override type TwitterApi = TwitterApiLike
    override val twitterEffect: scalaz.Monad[Task] = scalaz.Monad[Task]

    override implicit val TwitterCtx = java.util.concurrent.Executors.newFixedThreadPool(2)

    final class ScalazTaskApi extends TwitterApiLike {

      override def load(query: String): Task[ValidTweet] = Task(0.successNel[String])

      override def reduce(query: String): Task[ValidTweet] =
        Task {
          (1.successNel[String] :: 2.successNel[String] :: "3 error".failureNel[Tweet] :: "4 error".failureNel[Tweet] :: Nil).sequenceU
          .map(_.foldMap(t=>t)(implicitly[Monoid[Tweet]]))
        }(TwitterCtx)

      override protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazTaskApi()
  }

  trait ScalazFutureDbService extends DbServiceModule[Future] {
    override type Record = Int
    override type DbContext = scala.concurrent.ExecutionContext
    override type DbApi = ScalazFutureApi

    override implicit val DbCtx = scala.concurrent.ExecutionContext.Implicits.global
    override val dbEffect: scalaz.Monad[Future] = scalaz.Monad[Future]

    final class ScalazFutureApi extends DbApiLike {

      override def one(query: String) =
        Future {
          Thread.sleep(200)
          println(s"one:start ${Thread.currentThread().getName}")
          Thread.sleep(800)
          println(s"one:end ${Thread.currentThread().getName}")
          //throw new Exception("one error")
          0.successNel[String]
        }.recover {
          case ex: Throwable => ex.getMessage.failureNel[Record]
        }

      override def page(query: String): Future[ValidRecord] =
        Future {
          Thread.sleep(500)
          println(s"page:start ${Thread.currentThread().getName}")
          Thread.sleep(300)
          println(s"page:end ${Thread.currentThread().getName}")
          //throw new Exception("page error")
          (1.successNel[String] :: 2.successNel[String] :: "3 error".failureNel[Record] :: "4 error".failureNel[Record] :: Nil).sequenceU
            .map(_.foldMap(i=>i)(implicitly[Monoid[Record]]))
        }.recover {
          case ex: Throwable => ex.getMessage.failureNel[Record]
        }
    }

    override lazy val dbApi = new ScalazFutureApi()
  }

  trait ScalazTaskDbService extends DbServiceModule[Task] {
    override type Record = Int
    override type DbContext = java.util.concurrent.Executor //scalaz.concurrent.Strategy
    override val dbEffect: scalaz.Monad[Task] = scalaz.Monad[Task]
    override type DbApi = ScalazTaskApi

    implicit val DbCtx = java.util.concurrent.Executors.newFixedThreadPool(2)

    final class ScalazTaskApi extends DbApiLike {
      override def one(query: String) =
        Task(0)(DbCtx).attemptRun match {
          case \/-(r) => Task.now(r.successNel[String])
          case -\/(ex) => Task.now(ex.getMessage.failureNel[Record])
        }

      override def page(query: String): Task[ValidRecord] = {
        Task {
          (10.successNel[String] :: 11.successNel[String] :: "12 error".failureNel[Record] :: "13 error".failureNel[Record] :: Nil).sequenceU
            .map(_.foldMap(i=>i)(implicitly[Monoid[Record]]))
        }(DbCtx)
      }
    }

    override lazy val dbApi = new ScalazTaskApi()
  }

  object FutureDbService extends ScalazFutureDbService
  object ScalazFutureTwitterService extends ScalazFutureTwitter

  object TaskDbService extends ScalazTaskDbService
  object ScalazTaskTwitterService extends ScalazTaskTwitter

  object ApplicationFutureService extends ScalazFutureTwitter with ScalazFutureDbService {

    def zip =
      ((twitterApi reduce "select page") zip (dbApi page "select page"))
        .map(pair => (pair._1 |@| pair._2) {case (a, b) ⇒ s"twitter:$a db:$b"})(TwitterCtx)

    def zip2 = {
      (twitterApi reduce "select page").flatMap { a =>
        (dbApi page "select page").map { b =>
          ((a |@| b) { case (a, b) ⇒ s"twitter:$a db:$b" })
        }(DbCtx)
      }(TwitterCtx)
    }

    def zip3 = {
      import services.Services.Implicits._
      (services.Services[Int, scala.concurrent.Future] zip services.Services[String, scala.concurrent.Future])
        .map(pair => (pair._1 |@| pair._2) {case (a, b) ⇒ s"twitter:$a db:$b"})(TwitterCtx)
    }

  }

  object ApplicationTaskService extends ScalazTaskTwitter with ScalazTaskDbService {
    def zip = for {
      x <- (twitterApi reduce "select page")
      y <- (dbApi page "select page")
    } yield ((x |@| y){case (a, b) ⇒ s"twitter:$a db:$b"})
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