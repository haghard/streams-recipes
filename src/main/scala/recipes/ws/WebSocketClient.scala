package recipes.ws

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.stream.{ActorMaterializer, FlowShape, Graph, KillSwitches, SourceShape}
import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import recipes.ws.WindTurbineSimulator.{FailedUpgrade, Upgraded}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object WindTurbineData {

  def apply(id: String) = new WindTurbineData(id)
}

class WindTurbineData(id: String) {

  def getNext: String = {
    val timestamp  = System.currentTimeMillis / 1000
    val power      = f"${ThreadLocalRandom.current.nextDouble * 10}%.2f"
    val rotorSpeed = f"${ThreadLocalRandom.current.nextDouble * 10}%.2f"
    val windSpeed  = f"${ThreadLocalRandom.current.nextDouble * 100}%.2f"

    s"""{
       |    "id": "$id",
       |    "timestamp": $timestamp,
       |    "measurements": {
       |        "power": $power,
       |        "rotor_speed": $rotorSpeed,
       |        "wind_speed": $windSpeed
       |    }
       |}""".stripMargin
  }
}

object WebSocketClient {

  def apply(
    id: String,
    endpoint: String,
    supervisor: ActorRef
  )(implicit sys: ActorSystem, mat: ActorMaterializer, ec: ExecutionContext) =
    new WebSocketClient(id, endpoint, supervisor)(sys, mat, ec)

}

class WebSocketClient(id: String, endpoint: String, supervisor: ActorRef)(implicit
  system: ActorSystem,
  materializer: ActorMaterializer,
  executionContext: ExecutionContext
) {

  val webSocket: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = {
    val websocketUri = s"$endpoint/$id"
    //Http().cachedHostConnectionPool()

    //https://doc.akka.io/docs/akka-http/current/client-side/host-level.html
    //The connection pool underlying a pool client flow is cached.
    // There will never be more than a single pool live at any time.

    /*
      val address = Uri("google.com").authority.host.address()

      //A string is used to tag requests
      val poolClientFlow = Http().newHostConnectionPool[String](address, 8080)
      //val poolClientFlow = Http().cachedHostConnectionPool[String](address, 8080)
      val corelationId = UUID.randomUUID().toString
      val req: HttpRequest = null
      Source.single(req -> corelationId)
        .via(poolClientFlow)
        .recoverWithRetries(3, { case th: Throwable => ??? })
        .runForeach(???)
    */

    Http().webSocketClientFlow(WebSocketRequest(websocketUri))
  }

  //Streams telemetry to the service, once a second.
  val outgoing: Graph[SourceShape[Message], akka.NotUsed] = GraphDSL.create() { implicit b ⇒
    val data = WindTurbineData(id)

    val flow = b.add {
      Source
        .tick(1.seconds, 1.seconds, ())
        .map(_ ⇒ TextMessage(data.getNext))
    }

    SourceShape(flow.out)
  }

  //Life-cycle messages from server
  //Graph[SinkShape[Message], akka.NotUsed]
  val incoming: Graph[FlowShape[Message, Unit], akka.NotUsed] = GraphDSL.create() { implicit b ⇒
    val flow = b.add {
      Flow[Message]
        .collect {
          case TextMessage.Strict(text) ⇒
            Future.successful(text)
          case TextMessage.Streamed(textStream) ⇒
            textStream
              .runFold("")(_ + _)
              .flatMap(Future.successful)
        }
        .mapAsync(1)(identity)
        .map(println)
    }

    //akka.stream.SinkShape(flow.in)
    FlowShape(flow.in, flow.out)
  }

  /*Flow
    .fromSinkAndSourceCoupled(incoming, outgoing)
    .viaMat(KillSwitches.single)(Keep.right)*/

  val ((upgradeResponse, killSwitch), closed) =
    Source
      .fromGraph(outgoing)
      .viaMat(webSocket)(Keep.right)          // keep the materialized Future[WebSocketUpgradeResponse]
      .viaMat(KillSwitches.single)(Keep.both) // also keep the KillSwitch
      .via(incoming)
      .toMat(Sink.ignore)(Keep.both) // also keep the Future[Done]
      .run()

  upgradeResponse
    .map { upgrade ⇒
      upgrade.response.status match {
        case StatusCodes.SwitchingProtocols ⇒ supervisor ! Upgraded
        case statusCode                     ⇒ supervisor ! FailedUpgrade(statusCode)
      }
    }
    .onComplete {
      case Success(_)  ⇒ supervisor ! WindTurbineSimulator.Connected
      case Failure(ex) ⇒ supervisor ! WindTurbineSimulator.ConnectionFailure(ex)
    }

  closed.map(_ ⇒ supervisor ! WindTurbineSimulator.Terminated)
}
