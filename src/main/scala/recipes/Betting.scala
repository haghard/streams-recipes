package recipes

import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer, OverflowStrategy}
import com.typesafe.config.ConfigFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

//runMain recipes.Betting
object Betting extends App {

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

  trait Reply {
    def playerId: Long
  }

  case class BetPlacedReply(playerId: Long, amount: Int) extends Reply

  case class GTState(placedBets: Map[Long, Int] = Map.empty)
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
      ex.printStackTrace
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

  implicit val ec = mat.executionContext

  //fetch some data from cache
  def prefetchDataForValidation(ordCmd: OrderedCmd, state: GTState)(implicit
    ec: ExecutionContext
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
        case other ⇒ EnrichedCmd(0L, null, false)
      }
    }

  def preValidation(
    state: GTState,
    parallelism: Int = 4
  ): Flow[OrderedCmd, immutable.SortedSet[EnrichedCmd], akka.NotUsed] =
    Flow[OrderedCmd]
      .mapAsyncUnordered(parallelism) { cmd ⇒
        prefetchDataForValidation(cmd, state)(mat.executionContext)
      }
      .conflateWithSeed { enrichedData: EnrichedCmd ⇒
        immutable.SortedSet
          .empty[EnrichedCmd]((x: EnrichedCmd, y: EnrichedCmd) ⇒ if (x.seqNum < y.seqNum) -1 else 1) + enrichedData
      }({
        case (acc, enrichedData) ⇒ acc + enrichedData
      })

  /*def preProcessing0(state: GTState, bufferSize: Int = 1 << 8) =
    Source
      .queue[(GTState, OrderedCmd)](bufferSize, OverflowStrategy.dropHead)
      .mapAsyncUnordered(4) { case (state, cmd) =>
        prefetchDataForValidation(cmd, state)(mat.executionContext)
      }.conflateWithSeed({ enrichedData: EnrichedCmd ⇒
        immutable.SortedSet
          .empty[EnrichedCmd]((x: EnrichedCmd, y: EnrichedCmd) ⇒ if (x.seqNum < y.seqNum) -1 else 1) + enrichedData
      })({
        case (acc, enrichedData) ⇒ acc + enrichedData
      })*/

  def preProcessing(state: GTState, bufferSize: Int = 1 << 8) =
    Source
      .queue[Cmd](bufferSize, OverflowStrategy.dropHead)
      .scan(OrderedCmd(0L)) { (orderedCmd, cmd) ⇒
        orderedCmd.withOrigin(cmd)
      }
      .via(preValidation(state))
      //.zipWith(Source.tick(1.second, 1.second, ()))(Keep.left)
      .to(
        Sink.foreachAsync(1) { batch ⇒
          Future {
            batch.map {
              cmd ⇒
                //data from cache
                if (cmd.knownUser)
                  cmd.originCmd match {
                    case PlaceBet(pId, amount) ⇒
                      val maybeAmount = state.placedBets.get(pId)
                      val updatedState = maybeAmount match {
                        case Some(prevAmount) ⇒
                          if (amount + prevAmount < 1000) state.copy(state.placedBets.updated(pId, amount + prevAmount))
                          else state
                        case None ⇒
                          state.copy(state.placedBets + (pId → amount))
                      }
                      updatedState
                    case other ⇒
                  }
                else {}

                //
                cmd.originCmd

                //data from cache
                cmd.knownUser
            }

            //apply cmd batch:
            //ask to persist
            Thread.sleep(1000)
            println(s"[${batch.mkString(",")}]")
          }(mat.executionContext)
        }
      )
      .run()(mat)

  def producerLoop(startTs: Long, inlet: SourceQueueWithComplete[Cmd]): Unit = {
    val playerId = ThreadLocalRandom.current.nextInt(0, 20)
    inlet
      .offer(PlaceBet(playerId, 100))
      .onComplete { _ ⇒
        println(s"Offered bet from: $playerId")
        Thread.sleep(100)
        //produce for 10 sec
        if (System.currentTimeMillis - startTs < 10000L) producerLoop(startTs, inlet)
      }(mat.executionContext)
  }

  /*.toMat(Sink.foreach { batch ⇒
          Thread.sleep(1000)
          val tss = batch.map(_.ts).mkString(",")
          println(s"${batch.size} - ${tss}")
        })(Keep.left)
        .run()(mat)*/
  /*.toMat(Sink.foreach { case (reps, p) ⇒  p.complete(Success(reps)) })*/

  producerLoop(System.currentTimeMillis, preProcessing(GTState()))

}
