package recipes

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, OverflowStrategy}
import com.typesafe.config.ConfigFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

//runMain recipes.Betting
object Prevalidation extends App {

  trait Cmd {
    def playerId: Long
  }

  case class OrderedCmd(seqNum: Long, origin: Option[Cmd] = None) {
    def withOrigin(cmd: Cmd): OrderedCmd = copy(seqNum + 1L, origin = Some(cmd))
  }

  case class PlaceBet(playerId: Long, amount: Int) extends Cmd

  trait Event {
    def playerId: Long
  }

  case class BetPlaced(playerId: Long, amount: Int) extends Event

  //case class GTState(placedBets: Map[Long, Int] = Map.empty)
  case class EnrichedCmd(seqNum: Long, originCmd: Cmd, knownUser: Boolean)
  case class EmptyEnrichedCmd(seqNum: Long = 0L, originCmd: OrderedCmd = null, ts: Long = 0L, exist: Boolean = false)

  val config = ConfigFactory.parseString(
    """
       |akka {
       |  actor {
       |    default-dispatcher {
       |      type = Dispatcher
       |      executor = "fork-join-executor"
       |
       |      fork-join-executor {
       |        parallelism-min = 2
       |        parallelism-factor = 2.0
       |        parallelism-max = 6
       |      }
       |      throughput = 1
       |    }
       |  }
       |}
       |
     """.stripMargin
  )

  val decider: akka.stream.Supervision.Decider = {
    case ex: Throwable ⇒
      println(s"Caught error: ${ex.getMessage}")
      akka.stream.Supervision.Stop
  }

  implicit val sys: ActorSystem =
    ActorSystem("betting", ConfigFactory.empty().withFallback(config).withFallback(ConfigFactory.load()))

  val mat: Materializer = ActorMaterializer(
    ActorMaterializerSettings
      .create(system = sys)
      //.withInputBuffer(16, 16)
      .withSupervisionStrategy(decider)
      .withDispatcher("akka.actor.default-dispatcher")
  )

  //fetch some data from cache
  def asyncPreValidation(ordCmd: OrderedCmd)(
    implicit ec: ExecutionContext
  ): Future[EnrichedCmd] =
    Future {
      //println(s"start: ${ordCmd.seqNum}")
      ordCmd.origin match {
        case Some(cmd) ⇒
          //Fetch from cache using incoming cmd
          Thread.sleep(ThreadLocalRandom.current.nextInt(50, 100)) //

          //state.bets
          if (cmd.playerId > 0 && cmd.playerId < 10) EnrichedCmd(ordCmd.seqNum, cmd, true)
          else EnrichedCmd(ordCmd.seqNum, cmd, false)
        case _ ⇒ EnrichedCmd(0L, null, false)
      }
    }(ec)

  def preValidation(
    parallelism: Int = 4
  ): Flow[Cmd, immutable.SortedSet[EnrichedCmd], akka.NotUsed] =
    Flow[Cmd]
      .scan(OrderedCmd(0L)) { (ordCmd, cmd) ⇒
        ordCmd.withOrigin(cmd)
      }
      .mapAsyncUnordered(parallelism) { cmd ⇒
        asyncPreValidation(cmd)(mat.executionContext)
      }
      .conflateWithSeed({ enrichedCmd: EnrichedCmd ⇒
        immutable.SortedSet
          .empty[EnrichedCmd]((x: EnrichedCmd, y: EnrichedCmd) ⇒ if (x.seqNum < y.seqNum) -1 else 1) + enrichedCmd
      })({ _ + _ })

  def preValidationFlow(bufferSize: Int = 1 << 8) =
    Source
      .queue[Cmd](bufferSize, OverflowStrategy.dropHead)
      .via(preValidation())
      .to(
        Sink.foreachAsync(1) { batch ⇒
          Future {
            Thread.sleep(500)
            if (batch.size == 1 && batch.head.originCmd == null) sys.log.info("ignore empty element")
            else sys.log.info("persist:[{}]", batch.mkString(","))
          }(mat.executionContext)
        }
        /*Sink.foreachAsync(1) { batch ⇒
          import akka.pattern.ask
          import scala.concurrent.duration._
          implicit val t = akka.util.Timeout(1.second)
          //val actor: ActorRef = ???
          (actor ask batch).mapTo[???]
        }*/
      )
      .run()(mat)

  def producer(startTs: Long, process: SourceQueueWithComplete[Cmd]): Unit = {
    val playerId = ThreadLocalRandom.current.nextInt(0, 10)
    process
      .offer(PlaceBet(playerId, ThreadLocalRandom.current.nextInt(0, 100)))
      .onComplete { _ ⇒
        sys.log.debug("Offered bet from: {}", playerId)
        //persist
        Thread.sleep(100)
        if (System.currentTimeMillis - startTs < 10000L) producer(startTs, process)
      }(mat.executionContext)
  }

  val flow = preValidationFlow()
  producer(System.currentTimeMillis, flow)

}
