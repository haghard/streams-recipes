package object services {
  import scala.concurrent.duration._
  import scalaz.concurrent.{Task, Future}
  import scalaz._, Scalaz._

  trait ServiceModule[M[+_]] {
    type Result
    type Api <: ApiLike

    def M: scalaz.Monad[M]

    protected trait ApiLike {
      def load: M[Result]
      def take(n: Int): M[List[Result]]
      def reduce: M[Result]
    }

    def api: Api
  }

  abstract class ScalazFutureService extends ServiceModule[Future] {
    override val M: scalaz.Monad[Future] = scalaz.Monad[Future]
    override type Result = Int
    override type Api = ScalazFutureApi

    final class ScalazFutureApi extends ApiLike {
      override def load = Future[Result](0)

      override def reduce: Future[Result] =
        take(5).map(_.foldMap(i=>i)(implicitly[Monoid[Result]]))

      override def take(n: Int): Future[List[Result]] =
        Future(List(1,2,3,4,5))
    }

    override lazy val api = new ScalazFutureApi()
  }

  abstract class ScalazTaskService extends ServiceModule[Task] {
    override val M: scalaz.Monad[Task] = scalaz.Monad[Task]
    override type Result = Int
    override type Api = ScalazTaskApi

    final class ScalazTaskApi extends ApiLike {
      override def load: Task[Result] = ???
      override def reduce: Task[Result] = ???
      override def take(n: Int): Task[List[Result]] = ???
    }

    override lazy val api = new ScalazTaskApi()
  }

  object FutureService extends ScalazFutureService
  object TaskService extends ScalazTaskService


  FutureService.api.reduce
    .attemptRunFor(3 second).fold({ ex ⇒ println(ex.getMessage) }, { res ⇒ println(res) })
}