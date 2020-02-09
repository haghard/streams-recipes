package recipes

import java.net.InetSocketAddress
import java.util.concurrent.ThreadLocalRandom

import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, StageLogging}

object Sinks {

  //Constant delay
  final class GraphiteSink[T](name: String, delay: Long, override val address: InetSocketAddress)
      extends GraphStage[SinkShape[T]]
      with GraphiteMetrics {

    val in: Inlet[T]                 = Inlet("GraphiteSink")
    override val shape: SinkShape[T] = SinkShape(in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        override def preStart(): Unit =
          pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              if (delay > 0)
                Thread.sleep(delay) //

              val _ = grab(in)
              send(s"$name:1|c")
              //val e = grab(in)
              //println("out:" + e.toString)
              pull(in)
            }
          }
        )
      }
  }

  final class GraphiteSink3(name: String, delay: Long, override val address: InetSocketAddress)
      extends GraphStage[SinkShape[(Int, Int, Int)]]
      with GraphiteMetrics {

    private val in: Inlet[(Int, Int, Int)]         = Inlet("GraphiteSink")
    override val shape: SinkShape[(Int, Int, Int)] = SinkShape(in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        override def preStart(): Unit =
          pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            if (delay > 0)
              Thread.sleep(delay) //

            val _ = grab(in)
            send(s"$name:1|c")
            pull(in)
          }
        })
      }
  }

  //Degrade with each new message
  final class DegradingGraphiteSink[T](name: String, delayPerMsg: Long, override val address: InetSocketAddress)
      extends GraphStage[SinkShape[T]]
      with GraphiteMetrics {

    val in: Inlet[T]                 = Inlet("GraphiteSink")
    override val shape: SinkShape[T] = SinkShape(in)

    override protected def initialAttributes: Attributes =
      Attributes.name("deg-sink").and(ActorAttributes.dispatcher(AkkaRecipes.FixedDispatcher))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        var delay = 0L

        override def preStart(): Unit = pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val _ = grab(in)
              delay += delayPerMsg
              val latency = delayPerMsg + (delay / 1000)

              /*
              if (ThreadLocalRandom.current.nextDouble > 0.92)
                log.debug("{}: latency {}", Thread.currentThread.getName, latency)
               */
              //println(Thread.currentThread.getName + ": " + latency + "" + log.isDebugEnabled)

              Thread.sleep(latency, (delay % 1000).toInt)
              //log.debug("{}:  out {}", Thread.currentThread.getName, e)
              send(s"$name:1|c")
              //println(s"${Thread.currentThread().getName} fsink ${i}")
              pull(in)
            }
          }
        )
      }
  }
}
