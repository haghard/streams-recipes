package object services {
  import scala.concurrent.duration._
  import scalaz.concurrent.{Task, Future}
  import scalaz._, Scalaz._

  trait ServiceModule[M[_]] {
    type Result
    type ServiceApi <: ServiceApiLike

    def effect: scalaz.Monad[M]

    protected trait ServiceApiLike {
      def load: M[Result]
      def take(n: Int): M[List[Result]]
      def reduce: M[Result]
    }

    def api: ServiceApi
  }

  trait UserServiceModule[M[_]] {
    type UserResult
    type UserServiceApi <: UserServiceApiLike

    def userEffect: scalaz.Monad[M]

    protected trait UserServiceApiLike {
      def load: M[UserResult]
      def take(n: Int): M[List[UserResult]]
      def reduce: M[UserResult]
    }

    def userApi: UserServiceApi
  }

  trait ScalazFutureService extends ServiceModule[Future] {
    override val effect: scalaz.Monad[Future] = scalaz.Monad[Future]
    override type Result = Int
    override type ServiceApi = ScalazFutureApi

    final class ScalazFutureApi extends ServiceApiLike {
      override def load = Future[Result](0)

      override def reduce: Future[Result] =
        take(5).map(_.foldMap(i=>i)(implicitly[Monoid[Result]]))

      override def take(n: Int): Future[List[Result]] =
        Future(List(1,2,3,4,5))
    }

    override lazy val api = new ScalazFutureApi()
  }

  trait ScalazFutureUserService extends UserServiceModule[Future] {
    override val userEffect: scalaz.Monad[Future] = scalaz.Monad[Future]
    override type UserResult = String
    override type UserServiceApi = ScalazFutureApi

    final class ScalazFutureApi extends UserServiceApiLike {
      override def load = Future[UserResult]("0")

      override def reduce: Future[UserResult] =
        take(5).map(_.foldMap(i=>i)(implicitly[Monoid[UserResult]]))

      override def take(n: Int): Future[List[UserResult]] =
        Future(List("1","2","3","4","5"))
    }

    override lazy val userApi = new ScalazFutureApi()
  }

  trait ScalazTaskService extends ServiceModule[Task] {
    override val effect: scalaz.Monad[Task] = scalaz.Monad[Task]
    override type Result = Int
    override type ServiceApi = ScalazTaskApi

    final class ScalazTaskApi extends ServiceApiLike {
      override def load: Task[Result] = ???
      override def reduce: Task[Result] = ???
      override def take(n: Int): Task[List[Result]] = ???
    }

    override lazy val api = new ScalazTaskApi()
  }

  object FutureService extends ScalazFutureService
  object TaskService extends ScalazTaskService

  object ComposedFutureService extends ScalazFutureService with ScalazFutureUserService {
    def zip = for {
      x <- api.reduce
      y <- userApi.reduce
    } yield (x,y)
  }

  FutureService.effect.pure(1)

  ComposedFutureService.zip
    .attemptRunFor(1 second).fold({ ex ⇒ println(ex.getMessage) }, { r ⇒ println(r) })

  FutureService.api.reduce
    .attemptRunFor(1 second).fold({ ex ⇒ println(ex.getMessage) }, { r ⇒ println(r) })
}