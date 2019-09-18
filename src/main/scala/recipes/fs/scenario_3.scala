package recipes.fs

import cats.effect.{ExitCode, IO, IOApp}
import recipes.{GraphiteSupport, TimeWindows}
import dsl.InvariantsDsl._

/*

runMain recipes.fs.scenario_3

 */
object scenario_3 extends IOApp with TimeWindows with GraphiteSupport {

  object Preconditions extends BasicDsl with CheckProdDsl with CheckSpecDsl

  override def run(args: List[String]): IO[ExitCode] = {
    import Preconditions._

    val expOr = (uniqueProd("b", Set("b", "c")) || knownSpec(Some(21L), Map(21L → "a", 3L → "b")))
      .&&(uniqueSpec(1, Set(2, 3, 4, 6)))
    val io = expOr(catsIOops)

    io.redeem({ ex ⇒
      println("Error: " + ex.getMessage)
      ExitCode.Error
    }, { r ⇒
      println(s"Out: $r")
      ExitCode.Success
    })
  }
}
