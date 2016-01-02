
package object services {
  import scala.concurrent.Future
  import scalaz.concurrent.Task
  import scalaz._, Scalaz._

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val FutureMonad = new Monad[Future] {
    def point[A](a: => A): Future[A] = Future(a)
    def bind[A, B](fa: Future[A])(f: (A) => Future[B]): Future[B] = fa flatMap f
  }

  trait TwitterModule[M[_]] {
    type Tweet
    type TwitterApi <: TwitterApiLike[Tweet]
    type Result = ValidationNel[String, Tweet]

    def effect: scalaz.Monad[M]

    protected trait TwitterApiLike[T] {
      def load(query: String): M[Result]
      def reduce(query: String): M[Result]
      protected def take(n: Int): List[T]
    }

    def twitterApi: TwitterApi
  }

  trait ScalazFutureTwitter extends TwitterModule[Future] {
    override type Tweet = Int
    override type TwitterApi = ScalazFutureApi
    override val effect: scalaz.Monad[Future] = scalaz.Monad[Future]

    final class ScalazFutureApi extends TwitterApiLike[Tweet] {
      override def load(query: String): Future[Result] =
        Future(0.successNel[String])

      override def reduce(query: String): Future[Result] =
        Future(take(5).foldMap(i => i)(implicitly[Monoid[Tweet]]).successNel[String])

      protected def take(n: Int): List[Tweet] = List.range(0, n)
    }

    override lazy val twitterApi = new ScalazFutureApi()
  }


  trait DbServiceModule[M[_]] {
    type T
    type DbResult = ValidationNel[String, T]
    type DbApi <: DbApiLike[T]

    def dbEffect: scalaz.Monad[M]

    protected trait DbApiLike[T] {
      def one(query: String): M[DbResult]
      def page(query: String): M[DbResult]
    }

    def dbApi: DbApi
  }

  trait ScalazFutureDbService extends DbServiceModule[Future] {
    override type T = Int
    override val dbEffect: scalaz.Monad[Future] = scalaz.Monad[Future]
    override type DbApi = ScalazFutureApi

    final class ScalazFutureApi extends DbApiLike[T] {
      override def one(query: String) = {
        Future {
          Thread.sleep(200)
          println(s"one:start ${Thread.currentThread().getName}")
          Thread.sleep(800)
          println(s"one:end ${Thread.currentThread().getName}")
          //throw new Exception("one error")
          0.successNel[String]
        }.recover {
          case ex: Throwable => ex.getMessage.failureNel[T]
        }
      }

      override def page(query: String): Future[DbResult] =
        Future {
          Thread.sleep(500)
          println(s"page:start ${Thread.currentThread().getName}")
          Thread.sleep(300)
          println(s"page:end ${Thread.currentThread().getName}")
          //throw new Exception("page error")
          List(1, 2, 3, 4, 5)
        }.map(list => (list.foldMap(i => i)(implicitly[Monoid[T]])).successNel[String])
        .recover {
          case ex: Throwable => ex.getMessage.failureNel[T]
        }
    }

    override lazy val dbApi = new ScalazFutureApi()
  }

  trait ScalazTaskDbService extends DbServiceModule[Task] {
    override type T = Int
    override val dbEffect: scalaz.Monad[Task] = scalaz.Monad[Task]
    override type DbApi = ScalazTaskApi

    final class ScalazTaskApi extends DbApiLike[T] {
      override def one(query: String) =
        Task(0).attemptRun match {
          case \/-(r) => Task.now(r.successNel[String])
          case -\/(ex) => Task.now(ex.getMessage.failureNel[T])
        }

      override def page(query: String): Task[DbResult] = {
        Task {
          (10.successNel[String] :: 11.successNel[String] ::
            "12.fetch error".failureNel[T] ::
            "13 fetch error".failureNel[T] :: Nil)
             .sequenceU
            .map(_.foldMap(i=>i)(implicitly[Monoid[T]]))
        }
      }
    }

    override lazy val dbApi = new ScalazTaskApi()
  }

  object FutureDbService extends ScalazFutureDbService
  object TaskDbService extends ScalazTaskDbService

  object ApplicationFutureService extends ScalazFutureTwitter with ScalazFutureDbService {
    def zip = for {
      x <- twitterApi.reduce("select page")
      y <- dbApi.page("select page")
    } yield ((x |@| y){case (a, b) ⇒ s"twitterApi:$a dbApi:$b"})
  }

/*

  import scala.concurrent.ExecutionContext.Implicits.global
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

  c.await()
*/

}