package recipes

import akka.actor.ActorRef
import akka.stream._
import akka.stream.stage.GraphStageLogic.StageActor

import scala.collection.mutable
import akka.stream.stage._

import scala.reflect.ClassTag

object CustomStages {

  //(E[]) new Object[capacity];
  //new Array[AnyRef](findNextPositivePowerOfTwo(capacity)).asInstanceOf[Array[T]])
  //new Array[AnyRef](findNextPositivePowerOfTwo(capacity)).asInstanceOf[Array[T]])
  object RingBuffer {
    def nextPowerOfTwo(value: Int): Int =
      1 << (32 - Integer.numberOfLeadingZeros(value - 1))
  }

  class RingBuffer[T: scala.reflect.ClassTag] private(capacity: Int, mask: Int, buffer: Array[T]) {
    private var tail: Long = 0l
    private var head: Long = 0l

    def this(capacity: Int) {
      this(RingBuffer.nextPowerOfTwo(capacity), RingBuffer.nextPowerOfTwo(capacity) - 1,
        Array.ofDim[T](RingBuffer.nextPowerOfTwo(capacity)))
    }

    def offer(e: T): Boolean = {
      val wrapPoint = tail - capacity
      if (head <= wrapPoint) false
      else {
        val ind = tail.toInt & mask
        buffer(ind) = e
        tail = tail + 1
        true
      }
    }

    def poll(): Option[T] = {
      if (head >= tail) None
      else {
        val index = head.toInt & mask
        val element: T = buffer(index)
        buffer(index) = null.asInstanceOf[T]
        head = head + 1
        Some(element)
      }
    }

    override def toString =
      s"nextHead: [$head/${head.toInt & mask}] nextTail:[$tail/${tail.toInt & mask}] buffer: ${buffer.mkString(",")}"
  }

  /*
    Related links:
      https://doc.akka.io/docs/akka/current/stream/stream-customize.html
      https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

      Rate decoupled graph stages
   */
  class InternalBufferStage[A](maxSize: Int) extends GraphStage[FlowShape[A, A]] {
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

        def nonFull: Boolean =
          buffer.size < maxSize

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
            if (nonFull) {
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
            if (!nonFull && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        })
      }
  }

  class InternalCircularBuffer[A](maxSize: Int) extends GraphStage[FlowShape[A, A]] {
    val in = Inlet[A]("buffer.in")
    val out = Outlet[A]("buffer.out")

    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("bf").and(ActorAttributes.dispatcher("akka.flow-dispatcher"))

    //the main difference is that an onPush call does not always lead to calling push and an onPull call does not always lead to calling pull.
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var isDownstreamRequested = false
        val rb = new org.apache.commons.collections4.queue.CircularFifoQueue[A](maxSize)

        def watermarkIsReached: Boolean = rb.isFull

        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        override def preStart(): Unit =
          pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            rb.add(elem)

            //if (buffer.size > 1) println(s"buffer-size ${buffer.size}")

            if (isDownstreamRequested) {
              isDownstreamRequested = false
              val bufferedElem = rb.poll()
              push(out, bufferedElem)
            }

            if (!watermarkIsReached) {
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (!rb.isEmpty) {
              // emit the rest if possible
              import scala.collection.JavaConverters._
              emitMultiple(out, rb.iterator().asScala)
            }
            completeStage()
          }
        })

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (rb.isEmpty) {
              isDownstreamRequested = true
            } else {
              val elem = rb.poll()
              push(out, elem)
            }
            if (!watermarkIsReached && !hasBeenPulled(in)) {
              pull(in)
            }
          }
        })
      }
  }

  final class DisjunctionRouter[T, A](validationLogic: T ⇒ A Either T) extends GraphStage[FanOutShape2[T, A, T]] {
    val in = Inlet[T]("in")
    val error = Outlet[A]("error")
    val out = Outlet[T]("out")

    override def shape = new FanOutShape2[T, A, T](in, error, out)

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        var initialized = false
        var pending: Option[(A, Outlet[A]) Either (T, Outlet[T])] = None

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            pending = validationLogic(grab(in))
              .fold({ err: A ⇒ Option(Left(err, error)) }, { v: T ⇒ Option(Right(v, out)) })
            tryPush
          }
        })

        List(error, out).foreach {
          setHandler(_, new OutHandler {
            override def onPull() = {
              if (!initialized) {
                initialized = true
                tryPull(in)
              }
              tryPush
            }
          })
        }

        private def tryPushError(er: A, erOut: Outlet[A]): Unit = {
          if (isAvailable(erOut)) {
            push(erOut, er)
            tryPull(in)
            pending = None

            if (isClosed(in)) {
              completeStage()
            }
          }
        }

        private def tryPush(el: T, out: Outlet[T]): Unit = {
          if (isAvailable(out)) {
            push(out, el)
            tryPull(in)
            pending = None

            if (isClosed(in)) {
              completeStage()
            }
          }
        }

        private def tryPush(): Unit = {
          pending.foreach { out ⇒
            out.fold({ kv ⇒
              tryPushError(kv._1, kv._2)
            }, { kv ⇒
              tryPush(kv._1, kv._2)
            })
          }
        }
      }
  }

  final class RoundRobinStage4[T] extends GraphStage[FanOutShape4[T, T, T, T, T]] {
    val in = Inlet[T]("in")
    val outlets = Vector(Outlet[T]("out0"), Outlet[T]("out1"), Outlet[T]("out2"), Outlet[T]("out3"))
    var seqNum = 0
    val s = outlets.size

    override def shape =
      new FanOutShape4[T, T, T, T, T](in, outlets(0), outlets(1), outlets(2), outlets(3))

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        var pending: Option[(T, Outlet[T])] = None
        var initialized = false

        setHandler(in, new InHandler {
          override def onPush() = {
            pending = Some((grab(in), outlets(seqNum % s)))
            seqNum += 1
            tryPush()
          }

          override def onUpstreamFinish() = {
            if (pending.isEmpty) {
              completeStage()
            }
          }
        })

        outlets.foreach {
          setHandler(_, new OutHandler {
            override def onPull() = {
              if (!initialized) {
                initialized = true
                tryPull(in)
              }
              tryPush()
            }
          })
        }

        private def tryPush(): Unit = {
          pending.foreach {
            case (el, out) ⇒
              if (isAvailable(out)) {
                push(out, el)
                tryPull(in)
                pending = None

                if (isClosed(in)) {
                  completeStage()
                }
              }
          }
        }
      }
  }


  object ActorSource {
    case class ConnectSource(ref: ActorRef)
  }

  //A custom graph stage to create a Source using getActorStage
  //Example: https://github.com/Keenworks/SampleActorStage/blob/master/src/main/scala/com/keenworks/sample/sampleactorstage/MessageSource.scala

  //How to use: Source.fromGraph(new ActorSource[ByteString](sourceRef) ...
  final class ActorSource[T: ClassTag](sourceFeeder: ActorRef) extends GraphStage[SourceShape[T]] {
    val out: Outlet[T] = Outlet("out")
    override val shape: SourceShape[T] = SourceShape(out)

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        lazy val actorStage: StageActor = getStageActor(onReceive)
        val buffer = mutable.Queue[T]()

        override def preStart(): Unit = {
          sourceFeeder ! ActorSource.ConnectSource(actorStage.ref)
        }

        setHandler(out,
          new OutHandler {
            override def onDownstreamFinish(): Unit = {
              val result = buffer.result
              if (result.nonEmpty) {
                /*
                log.debug(
                  "In order to avoid message lost we need to notify the upsteam that " +
                    "consumed elements cannot be handled")
                1. actor ! result - resend maybe
                2. store to internal DB
                */
                completeStage()
              }
              completeStage()
            }

            override def onPull(): Unit = tryPush()
          }
        )

        def tryPush(): Unit = {
          if (isAvailable(out) && buffer.nonEmpty) {
            val element = buffer.dequeue
            push(out, element)
          }
        }

        def onReceive(x: (ActorRef, Any)): Unit = {
          x._2 match {
            case msg: T =>
              buffer enqueue msg
              tryPush()
            case other =>
              failStage(throw new Exception(s"Unexpected message type ${other.getClass.getSimpleName}"))
          }
        }
      }
  }

}