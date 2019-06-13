package recipes

import java.io.{ File, FileOutputStream }

import akka.actor.ActorRef
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.stage.GraphStageLogic.StageActor

import scala.collection.{ immutable, mutable }
import akka.stream.stage._
import akka.util.ByteString

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.reflect.ClassTag

object CustomStages {

  //(E[]) new Object[capacity];
  //new Array[AnyRef](findNextPositivePowerOfTwo(capacity)).asInstanceOf[Array[T]])
  //new Array[AnyRef](findNextPositivePowerOfTwo(capacity)).asInstanceOf[Array[T]])

  object RingBuffer {
    def nextPowerOfTwo(value: Int): Int =
      1 << (32 - Integer.numberOfLeadingZeros(value - 1))
  }

  //https://en.wikipedia.org/wiki/Moving_average
  //https://blog.scalac.io/2017/05/25/scala-specialization.html
  class SimpleRingBuffer[@specialized(Double, Long, Int) T: SourceType: ClassTag: Numeric] private (capacity: Int, buffer: Array[T]) {
    private var tail: Long = 0l
    private var head: Long = 0l

    def this(capacity: Int) =
      this(capacity, Array.ofDim[T](capacity))

    def add(e: T): Unit = {
      val ind = (tail % capacity).toInt

      buffer(ind) = e
      tail += 1

      val wrapPoint = tail - capacity
      if (head <= wrapPoint)
        head = tail - capacity
    }

    def currentHead: T = {
      val ind = (head % capacity).toInt
      buffer(ind)
    }

    def sum: Double =
      implicitly[SourceType[T]].apply(buffer.sum)

    def size(): Int = (tail - head).toInt

    override def toString =
      s"head: [$head] tail:$tail buffer: ${buffer.mkString(",")}"
  }

  class RingBuffer[T: scala.reflect.ClassTag] private (capacity: Int, mask: Int, buffer: Array[T]) {
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
        val ind = (tail & mask).toInt
        buffer(ind) = e
        tail = tail + 1
        true
      }
    }

    def poll(): Option[T] = {
      if (head >= tail) None
      else {
        val index = (head & mask).toInt
        val element: T = buffer(index)
        buffer(index) = null.asInstanceOf[T]
        head = head + 1
        Some(element)
      }
    }

    def peek(): Option[T] = {
      if (head >= tail) None
      else {
        val index = head.toInt & mask
        Some(buffer(index))
      }
    }

    def size() = (tail - head).toInt

    override def toString =
      s"nextHead: [$head/${head.toInt & mask}] nextTail:[$tail/${tail.toInt & mask}] buffer: ${buffer.mkString(",")}"
  }

  /*
    Related links:
      https://doc.akka.io/docs/akka/current/stream/stream-customize.html
      https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

      Rate decoupled graph stages.
      The main point being is that an onPush call does not always lead to calling push and
        an onPull call does not always lead to calling pull.
   */
  class InternalBufferStage[A](maxSize: Int) extends GraphStage[FlowShape[A, A]] {
    val in = Inlet[A]("ib.in")
    val out = Outlet[A]("ib.out")
    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("ib")
        //.and(ActorAttributes.dispatcher("akka.blocking-dispatcher"))
        .and(ActorAttributes.dispatcher("akka.flow-dispatcher"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        //this if how we can access internal decider
        val decider = inheritedAttributes.get[akka.stream.ActorAttributes.SupervisionStrategy]
          .map(_.decider).getOrElse(Supervision.stoppingDecider)

        var isDownstreamRequested = false
        val fifo = mutable.Queue[A]()

        def watermarkIsNotReached: Boolean =
          fifo.size < maxSize

        // a detached stage needs to start upstream demand itself as it's not triggered by downstream demand
        override def preStart(): Unit =
          pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            fifo enqueue elem //O(1)

            /*
            FOR DEBUG
            if (buffer.size > 1)
              println(s"buffer-size ${buffer.size}")
            */

            if (isDownstreamRequested) {
              isDownstreamRequested = false
              val bufferedElem = fifo.dequeue
              push(out, bufferedElem)
            }

            if (watermarkIsNotReached)
              pull(in)

          }

          override def onUpstreamFinish(): Unit = {
            if (fifo.nonEmpty) {
              // emit the rest if possible
              emitMultiple(out, fifo.toIterator)
            }
            completeStage()
          }
        })

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (fifo.isEmpty) {
              isDownstreamRequested = true
            } else {
              val elem = fifo.dequeue //O(1)
              push(out, elem)
            }
            if (!watermarkIsNotReached && !hasBeenPulled(in)) {
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

  final class DisjunctionStage[T, A](validation: T ⇒ A Either T) extends GraphStage[FanOutShape2[T, A, T]] {
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
            pending = validation(grab(in))
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

        private def tryPushResult(el: T, out: Outlet[T]): Unit = {
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
              tryPushResult(kv._1, kv._2)
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

  object ActorBasedSource {

    case class ConnectSource(ref: ActorRef)

  }

  //A custom graph stage to create a Source using getActorStage
  //As a result you're able to send messages to an actor(sourceFeeder) and propagate them to a Source.
  //Example: https://github.com/Keenworks/SampleActorStage/blob/master/src/main/scala/com/keenworks/sample/sampleactorstage/MessageSource.scala
  /*

    How to use:
      Source.fromGraph(new ActorSource[ByteString](sourceRef) ...

      sourceRef ! message


   */
  final class ActorBasedSource[T: ClassTag](sourceFeeder: ActorRef) extends GraphStage[SourceShape[T]] {
    val out: Outlet[T] = Outlet("out")
    override val shape: SourceShape[T] = SourceShape(out)

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        lazy val actorStage: StageActor = getStageActor(onReceive)
        val buffer = mutable.Queue[T]()

        override def preStart(): Unit = {
          sourceFeeder ! ActorBasedSource.ConnectSource(actorStage.ref)
        }

        setHandler(
          out,
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
          })

        def tryPush(): Unit = {
          if (isAvailable(out) && buffer.nonEmpty) {
            val element = buffer.dequeue
            push(out, element)
          }
        }

        def onReceive(x: (ActorRef, Any)): Unit = {
          x._2 match {
            case msg: T ⇒
              buffer enqueue msg
              tryPush()
            case other ⇒
              failStage(throw new Exception(s"Unexpected message type ${other.getClass.getSimpleName}"))
          }
        }
      }
  }

  sealed trait SessionOverflowStrategy

  case object DropOldest extends SessionOverflowStrategy

  case object DropNewest extends SessionOverflowStrategy

  case object FailStage extends SessionOverflowStrategy

  final case class Overflow(msg: String) extends RuntimeException(msg)

  //https://efekahraman.github.io/2019/01/session-windows-in-akka-streams
  //https://softwaremill.com/windowing-data-in-akka-streams/
  //https://github.com/efekahraman/akka-streams-session-window
  //Session windows represent a period of activity. Concretely, windows are separated by a predefined inactivity/gap period.

  /*

  val window: GraphStage[FlowShape[Int, Int]] = SessionWindow(1 second, 10, FailStage)
  Source(range)
    .via(sessionWindow)
    .runFold(Nil: List[Int])(_ :+ _)
  */
  final class SessionWindow[T](inactivity: FiniteDuration, maxSize: Int, overflowStrategy: SessionOverflowStrategy)
    extends GraphStage[FlowShape[T, T]] {

    import scala.collection.immutable.Queue

    require(maxSize > 1, "maxSize must be greater than 1")
    require(inactivity > Duration.Zero)

    val in: Inlet[T] = Inlet[T]("SessionWindow.in")
    val out: Outlet[T] = Outlet[T]("SessionWindow.out")

    override val shape: FlowShape[T, T] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {

        private var queue: Queue[T] = Queue.empty[T]
        private var nextDeadline: Long = System.nanoTime + inactivity.toNanos

        setHandlers(in, out, this)

        override def preStart(): Unit =
          schedulePeriodically(shape, inactivity)

        override def postStop(): Unit =
          queue = Queue.empty[T]

        override def onPush(): Unit = {
          val element: T = grab(in)
          queue =
            if (queue.size < maxSize) queue.enqueue(element)
            else
              overflowStrategy match {
                case DropOldest ⇒
                  if (queue.isEmpty) queue.enqueue(element)
                  else queue.tail.enqueue(element)
                case DropNewest ⇒ queue
                case FailStage ⇒
                  failStage(Overflow(s"Received messages are more than $maxSize"))
                  Queue.empty[T]
              }

          nextDeadline = System.nanoTime + inactivity.toNanos

          if (!hasBeenPulled(in)) pull(in)
        }

        override def onPull(): Unit =
          if (!hasBeenPulled(in)) pull(in)

        override def onUpstreamFinish(): Unit = {
          if (queue.nonEmpty) {
            emitMultiple(out, queue)
          }
          super.onUpstreamFinish()
        }

        final override protected def onTimer(key: Any): Unit =
          if (nextDeadline - System.nanoTime < 0 && queue.nonEmpty) {
            emitMultiple(out, queue)
            queue = Queue.empty[T]
          }
      }
  }

  //http://blog.kunicki.org/blog/2016/07/20/implementing-a-custom-akka-streams-graph-stage/
  final class AccumulateWhileUnchanged[E, P](propertyExtractor: E ⇒ P) extends GraphStage[FlowShape[E, immutable.Seq[E]]] {
    val in = Inlet[E]("in")
    val out = Outlet[immutable.Seq[E]]("out")

    override def shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        private var currentState: Option[P] = None
        private val buffer = Vector.newBuilder[E]

        setHandlers(in, out, new InHandler with OutHandler {
          override def onPush(): Unit = {
            val nextElement = grab(in)
            val nextState = propertyExtractor(nextElement)

            if (currentState.isEmpty || currentState.contains(nextState)) {
              buffer += nextElement
              pull(in)
            } else {
              val result = buffer.result
              buffer.clear()
              buffer += nextElement
              push(out, result)
            }

            currentState = Some(nextState)
          }

          override def onPull(): Unit = pull(in)

          override def onUpstreamFinish(): Unit = {
            val result = buffer.result()
            if (result.nonEmpty) {
              emit(out, result)
            }
            completeStage()
          }
        })

        override def postStop(): Unit = {
          buffer.clear()
        }
      }
  }

  // Stage to measure backpressure
  //https://youtu.be/4s1YzgrRR2A?list=PLbZ2T3O9BuvczX5j03bWMrMFzK5OAs9mZ
  final class BackpressureMeasurementStage[In, Out](f: In ⇒ Out) extends GraphStage[FlowShape[In, Out]] {
    val in = Inlet[In]("map.in")
    val out = Outlet[Out]("map.out")

    override val shape: FlowShape[In, Out] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler {
        val histogram = new org.HdrHistogram.Histogram(3600000000000L, 3)

        var lastPulled: Long = System.nanoTime
        var lastPushed: Long = lastPulled

        //

        override def onPush(): Unit = {
          push(out, f(grab(in)))
          val now = System.nanoTime
          //ratio between time spent for the pull and time spent for the push (percentage)
          // 0 percent means -- never backpressured
          // 100 percent means -- always backpressured
          val v = (lastPulled - lastPulled) * 100 / now - lastPushed
          histogram.recordValue(v)
          lastPushed = now
        }

        override def onPull(): Unit = {
          pull(in)
          lastPulled = System.nanoTime
        }

        override def postStop(): Unit = {
          val out = new FileOutputStream(new File("./histograms/" + System.currentTimeMillis + ".txt"))
          val d = (1000000.0).asInstanceOf[java.lang.Double]
          histogram.outputPercentileDistribution(new java.io.PrintStream(out), d) //in millis
        }
      }
  }

  /*
    Flow.fromGraph(new Chunker(chunkSize))
  */
  class Chunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in = Inlet[ByteString]("chunker.in")
    val out = Outlet[ByteString]("chunker.out")

    override val shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private var buffer = ByteString.empty

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          //println("Buffer size: " + buffer.size)
          emitChunk()
      })

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            buffer ++= elem
            emitChunk()
          }

          override def onUpstreamFinish(): Unit =
            if (buffer.isEmpty) completeStage()
            else {
              if (isAvailable(out)) emitChunk()
            }
        })

      private def emitChunk(): Unit =
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pull(in)
        } else {
          val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
          buffer = nextBuffer
          push(out, chunk)
        }
    }
  }

}