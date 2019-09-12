package recipes

import java.net.InetSocketAddress

import akka.stream.{ActorAttributes, Attributes, Inlet, SinkShape}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}

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

    private val in: Inlet[T]         = Inlet("GraphiteSink")
    override val shape: SinkShape[T] = SinkShape(in)

    override protected def initialAttributes: Attributes =
      Attributes.name("bf").and(ActorAttributes.dispatcher("akka.blocking-dispatcher"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        var delay = 0L

        override def preStart(): Unit =
          pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val _ = grab(in)
              delay += delayPerMsg
              val latency = delayPerMsg + (delay / 1000)
              Thread.sleep(latency, (delay % 1000).toInt)
              send(s"$name:1|c")
              //println(s"${Thread.currentThread().getName} fsink ${i}")
              pull(in)
            }
          }
        )
      }
  }
}
