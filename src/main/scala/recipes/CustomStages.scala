package recipes

import akka.stream._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }

import scala.collection.mutable

object CustomStages {

  /*
    Related links:
      https://doc.akka.io/docs/akka/current/stream/stream-customize.html
      https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

      Rate decoupled graph stages
   */
  class InternalBuffer[A](maxSize: Int) extends GraphStage[FlowShape[A, A]] {
    val in = Inlet[A]("buffer.in")
    val out = Outlet[A]("buffer.out")

    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("bf").and(ActorAttributes.dispatcher("akka.flow-dispatcher"))

    //the main difference is that an onPush call does not always lead to calling push and an onPull call does not always lead to calling pull.
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var isDownstreamRequested = false
        val buffer = mutable.Queue[A]()

        def watermarkIsReached: Boolean =
          buffer.size == maxSize

        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        override def preStart(): Unit =
          pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            buffer enqueue elem //O(1)

            //if (buffer.size > 1) println(s"buffer-size ${buffer.size}")

            if (isDownstreamRequested) {
              isDownstreamRequested = false
              val bufferedElem = buffer.dequeue
              push(out, bufferedElem)
            }
            if (!watermarkIsReached) {
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.nonEmpty) {
              // emit the rest if possible
              emitMultiple(out, buffer.toIterator)
            }
            completeStage()
          }
        })

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (buffer.isEmpty) {
              isDownstreamRequested = true
            } else {
              val elem = buffer.dequeue //O(1)
              push(out, elem)
            }
            if (!watermarkIsReached && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        })
      }
  }
}
