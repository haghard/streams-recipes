package recipes.ws

import WindTurbineSimulator._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.StatusCode
import akka.actor.{Actor, ActorLogging, Props}

//https://blog.colinbreck.com/integrating-akka-streams-and-akka-actors-part-ii/

//The following actor simulates a wind turbine
object WindTurbineSimulator {

  def props(id: String, endpoint: String)(implicit materializer: ActorMaterializer) =
    Props(classOf[WindTurbineSimulator], id, endpoint, materializer)

  final case object Upgraded
  final case object Connected
  final case object Terminated
  final case class ConnectionFailure(ex: Throwable)
  final case class FailedUpgrade(statusCode: StatusCode)
  final case class WindTurbineSimulatorException(id: String) extends Exception(s"Turbine:$id error")

}

class WindTurbineSimulator(id: String, endpoint: String)(implicit mat: ActorMaterializer)
    extends Actor
    with ActorLogging {
  implicit val system = context.system
  implicit val ec     = system.dispatcher

  //creates a WebSocket connection
  val webSocket = WebSocketClient(id, endpoint, self)

  override def postStop() = {
    log.info(s"$id : Stopping WebSocket connection")
    webSocket.killSwitch.shutdown()
  }

  override def receive: Receive = {
    case Upgraded ⇒
      log.info(s"$id : WebSocket upgraded")
    case FailedUpgrade(statusCode) ⇒
      log.error(s"$id : Failed to upgrade WebSocket connection : $statusCode")
      throw WindTurbineSimulatorException(id)
    case ConnectionFailure(ex) ⇒
      log.error(s"$id : Failed to establish WebSocket connection $ex")
      throw WindTurbineSimulatorException(id)
    case Connected ⇒
      log.info(s"$id : WebSocket connected")
      context become active
  }

  def active: Receive = { case Terminated ⇒
    log.error(s"$id : WebSocket connection terminated")
    throw WindTurbineSimulatorException(id)
  }
}
