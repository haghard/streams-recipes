package recipes

import java.io.{File, FileOutputStream}
import java.util.concurrent.ThreadLocalRandom

import akka.actor.ActorRef
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream._
import akka.stream.scaladsl.SinkQueueWithCancel
import akka.stream.stage.GraphStageLogic.StageActor

import scala.collection.{immutable, mutable}
import akka.stream.stage.{StageLogging, _}
import akka.util.ByteString
import recipes.AkkaRecipes.FixedDispatcher

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

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
  class SimpleRingBuffer[@specialized(Double, Long, Int) T: SourceElement: ClassTag: Numeric] private (
    capacity: Int,
    buffer: Array[T]
  ) {
    private var tail: Long = 0L
    private var head: Long = 0L
    private val d          = implicitly[SourceElement[T]]

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

    def sum: Double = d(buffer.sum)

    def size(): Int = (tail - head).toInt

    override def toString =
      s"[head:$head tail:$tail]: [${buffer.mkString(",")}]"
  }

  class RingBuffer[T: scala.reflect.ClassTag] private (capacity: Int, mask: Int, buffer: Array[T]) {
    private var tail: Long = 0L
    private var head: Long = 0L

    def this(capacity: Int) {
      this(
        RingBuffer.nextPowerOfTwo(capacity),
        RingBuffer.nextPowerOfTwo(capacity) - 1,
        Array.ofDim[T](RingBuffer.nextPowerOfTwo(capacity))
      )
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

    def poll(): Option[T] =
      if (head >= tail) None
      else {
        val index      = (head & mask).toInt
        val element: T = buffer(index)
        buffer(index) = null.asInstanceOf[T]
        head = head + 1
        Some(element)
      }

    def peek(): Option[T] =
      if (head >= tail) None
      else {
        val index = head.toInt & mask
        Some(buffer(index))
      }

    def size() = (tail - head).toInt

    override def toString =
      s"nextHead: [$head/${head.toInt & mask}] nextTail:[$tail/${tail.toInt & mask}] buffer: ${buffer.mkString(",")}"
  }

  /*
    Related links:
      https://doc.akka.io/docs/akka/current/stream/stream-customize.html
      https://doc.akka.io/docs/akka/current/stream/stream-customize.html#rate-decoupled-operators
      https://github.com/mkubala/akka-stream-contrib/blob/feature/101-mkubala-interval-based-rate-limiter/contrib/src/main/scala/akka/stream/contrib/IntervalBasedRateLimiter.scala

      Rate decoupled graph stages.
      The main point being is that an onPush call does not always lead to calling push and
        an onPull call does not always lead to calling pull.
   */
  final class BackPressuredStage[A](watermark: Int) extends GraphStage[FlowShape[A, A]] {
    val in    = Inlet[A]("ib.in")
    val out   = Outlet[A]("ib.out")
    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("back-pressured-buffer")
    //.and(ActorAttributes.dispatcher(FixedDispatcher))
    //.and(ActorAttributes.dispatcher("akka.flow-dispatcher"))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        //this if how we can access internal decider
        val decider = inheritedAttributes
          .get[akka.stream.ActorAttributes.SupervisionStrategy]
          .map(_.decider)
          .getOrElse(Supervision.stoppingDecider)

        var isDownstreamRequested = false
        val fifo                  = mutable.Queue[A]()

        def enoughSpace: Boolean =
          fifo.size < watermark

        // a detached stage needs to start upstream demand itself as it's not triggered by downstream demand
        override def preStart(): Unit = pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              fifo enqueue elem //O(1)

              //Can't keep up with req rate!
              //if (fifo.size > 1) log.debug("{} Buffering: {}", Thread.currentThread.getName, fifo.size)

              if (isDownstreamRequested) {
                isDownstreamRequested = false
                val elem = fifo.dequeue
                push(out, elem)
              }

              if (enoughSpace) pull(in)
              //wait for demand from downstream
              //else log.debug("{} Buffer is filled up. Wait for demand from the downstream", Thread.currentThread.getName)
            }

            override def onUpstreamFinish(): Unit = {
              if (fifo.nonEmpty) {
                // emit the rest if possible
                emitMultiple(out, fifo.iterator)
              }
              completeStage()
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = {
              if (fifo.isEmpty) {
                isDownstreamRequested = true
              } else {
                val elem = fifo.dequeue //O(1)
                //emitMultiple()
                push(out, elem)
              }

              if (enoughSpace && !hasBeenPulled(in)) {
                //log.debug("Downstream triggers pull {}", fifo.size)
                pull(in)
              }
            }
          }
        )
      }
  }

  final class DropHeadStage[A](watermark: Int) extends GraphStage[FlowShape[A, A]] {
    val in    = Inlet[A]("ib.in")
    val out   = Outlet[A]("ib.out")
    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("drop-head-buffer")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        //this if how we can access internal decider
        val decider = inheritedAttributes
          .get[akka.stream.ActorAttributes.SupervisionStrategy]
          .map(_.decider)
          .getOrElse(Supervision.stoppingDecider)

        var isDownstreamRequested = false
        val fifo                  = mutable.Queue[A]()

        def enoughSpace: Boolean =
          fifo.size < watermark

        // a detached stage needs to start upstream demand itself as it's not triggered by downstream demand
        override def preStart(): Unit = pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val inElem = grab(in)

              if (enoughSpace) {
                //log.debug("{} Buffering: {}", Thread.currentThread.getName, fifo.size)
                fifo.enqueue(inElem) //O(1)
                if (isDownstreamRequested) {
                  isDownstreamRequested = false
                  val outElem = fifo.dequeue
                  push(out, outElem)
                }
              } else {
                //log.debug("{} Ignoring: {}", Thread.currentThread.getName, inElem)
              }
              pull(in)
              //if (fifo.size > 1) log.debug("{} Can't keep up with req rate! Buffering: {}", Thread.currentThread.getName, fifo.size)
            }

            override def onUpstreamFinish(): Unit = {
              if (fifo.nonEmpty) {
                // emit the rest if possible
                emitMultiple(out, fifo.iterator)
              }
              completeStage()
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit =
              if (fifo.isEmpty) {
                isDownstreamRequested = true
              } else {
                val elem = fifo.dequeue //O(1)
                push(out, elem)
              }
          }
        )
      }
  }

  final class DropTailStage[A](watermark: Int) extends GraphStage[FlowShape[A, A]] {
    val in    = Inlet[A]("ib.in")
    val out   = Outlet[A]("ib.out")
    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("drop-tail-buffer")

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        //this if how we can access internal decider
        val decider = inheritedAttributes
          .get[akka.stream.ActorAttributes.SupervisionStrategy]
          .map(_.decider)
          .getOrElse(Supervision.stoppingDecider)

        var isDownstreamRequested = false
        val fifo                  = mutable.Queue[A]()

        def enoughSpace: Boolean =
          fifo.size < watermark

        // a detached stage needs to start upstream demand itself as it's not triggered by downstream demand
        override def preStart(): Unit = pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              fifo enqueue elem //O(1)

              //if (fifo.size > 1) log.debug("{} Can't keep up with req rate! Buffering: {}", Thread.currentThread.getName, fifo.size)

              if (isDownstreamRequested) {
                isDownstreamRequested = false
                val elem = fifo.dequeue
                push(out, elem)
              }

              if (enoughSpace) pull(in)
              else {
                val e = fifo.dequeue
                //log.debug("{} Dropping: {}", Thread.currentThread.getName, e)
                pull(in)
              }
            }

            override def onUpstreamFinish(): Unit = {
              if (fifo.nonEmpty) {
                // emit the rest if possible
                emitMultiple(out, fifo.iterator)
              }
              completeStage()
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit =
              if (fifo.isEmpty) {
                isDownstreamRequested = true
              } else {
                val elem = fifo.dequeue //O(1)
                push(out, elem)
              }
          }
        )
      }
  }

  class InternalCircularBuffer[A](maxSize: Int) extends GraphStage[FlowShape[A, A]] {
    val in  = Inlet[A]("buffer.in")
    val out = Outlet[A]("buffer.out")

    val shape = FlowShape.of(in, out)

    override protected def initialAttributes: Attributes =
      Attributes.name("bf").and(ActorAttributes.dispatcher("akka.flow-dispatcher"))

    //the main difference is that an onPush call does not always lead to calling push and an onPull call does not always lead to calling pull.
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {

        var isDownstreamRequested = false
        val rb                    = new org.apache.commons.collections4.queue.CircularFifoQueue[A](maxSize)

        def watermarkIsReached: Boolean = rb.isFull

        // a detached stage needs to start upstream demand
        // itself as it is not triggered by downstream demand
        override def preStart(): Unit =
          pull(in)

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val elem = grab(in)
              rb.add(elem)

              //if (buffer.size > 1) println(s"buffer-size ${buffer.size}")

              if (isDownstreamRequested) {
                isDownstreamRequested = false
                val bufferedElem = rb.poll()
                push(out, bufferedElem)
              }

              if (watermarkIsReached)
                //drop element
                rb.poll()

              pull(in)
            }

            override def onUpstreamFinish(): Unit = {
              if (!rb.isEmpty) {
                // emit the rest if possible
                import scala.jdk.CollectionConverters._
                emitMultiple(out, rb.iterator.asScala)
              }
              completeStage()
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
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
          }
        )
      }
  }

  final class DisjunctionStage[T, A](validation: T ⇒ A Either T) extends GraphStage[FanOutShape2[T, A, T]] {
    val in    = Inlet[T]("in")
    val error = Outlet[A]("error")
    val out   = Outlet[T]("out")

    override def shape = new FanOutShape2[T, A, T](in, error, out)

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        var initialized                                           = false
        var pending: Option[(A, Outlet[A]) Either (T, Outlet[T])] = None

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            pending = validation(grab(in))
              .fold({ err: A ⇒
                Option(Left(err, error))
              }, { v: T ⇒
                Option(Right(v, out))
              })
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

        private def tryPushError(er: A, erOut: Outlet[A]): Unit =
          if (isAvailable(erOut)) {
            push(erOut, er)
            tryPull(in)
            pending = None

            if (isClosed(in)) {
              completeStage()
            }
          }

        private def tryPushResult(el: T, out: Outlet[T]): Unit =
          if (isAvailable(out)) {
            push(out, el)
            tryPull(in)
            pending = None

            if (isClosed(in)) {
              completeStage()
            }
          }

        private def tryPush(): Unit =
          pending.foreach { out ⇒
            out.fold({ kv ⇒
              tryPushError(kv._1, kv._2)
            }, { kv ⇒
              tryPushResult(kv._1, kv._2)
            })
          }
      }
  }

  final class RoundRobinStage4[T] extends GraphStage[FanOutShape4[T, T, T, T, T]] {
    val in      = Inlet[T]("in")
    val outlets = Vector(Outlet[T]("out0"), Outlet[T]("out1"), Outlet[T]("out2"), Outlet[T]("out3"))
    var seqNum  = 0
    val s       = outlets.size

    override def shape =
      new FanOutShape4[T, T, T, T, T](in, outlets(0), outlets(1), outlets(2), outlets(3))

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        var pending: Option[(T, Outlet[T])] = None
        var initialized                     = false

        setHandler(
          in,
          new InHandler {
            override def onPush() = {
              pending = Some((grab(in), outlets(seqNum % s)))
              seqNum += 1
              tryPush()
            }

            override def onUpstreamFinish() =
              if (pending.isEmpty) {
                completeStage()
              }
          }
        )

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

        private def tryPush(): Unit =
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
    val out: Outlet[T]                 = Outlet("out")
    override val shape: SourceShape[T] = SourceShape(out)

    override def createLogic(attributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        lazy val actorStage: StageActor = getStageActor(onReceive)
        val buffer                      = mutable.Queue[T]()

        override def preStart(): Unit =
          sourceFeeder ! ActorBasedSource.ConnectSource(actorStage.ref)

        setHandler(
          out,
          new OutHandler {
            override def onDownstreamFinish(): Unit = {
              if (buffer.nonEmpty) {
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

        def tryPush(): Unit =
          if (isAvailable(out) && buffer.nonEmpty) {
            val element = buffer.dequeue
            push(out, element)
          }

        def onReceive(x: (ActorRef, Any)): Unit =
          x._2 match {
            case msg: T ⇒
              buffer enqueue msg
              tryPush()
            case other ⇒
              failStage(throw new Exception(s"Unexpected message type ${other.getClass.getSimpleName}"))
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
  val sessionWindow: GraphStage[FlowShape[Int, Int]] = SessionWindow(1.second, 10, FailStage)
  Source(range)
    .via(sessionWindow)
    .runFold(Nil: List[Int])(_ :+ _)
   */
  final class SessionWindow[T](inactivity: FiniteDuration, maxSize: Int, overflowStrategy: SessionOverflowStrategy)
      extends GraphStage[FlowShape[T, T]] {

    import scala.collection.immutable.Queue

    require(maxSize > 1, "maxSize must be greater than 1")
    require(inactivity > Duration.Zero)

    val in: Inlet[T]   = Inlet[T]("SessionWindow.in")
    val out: Outlet[T] = Outlet[T]("SessionWindow.out")

    override val shape: FlowShape[T, T] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new TimerGraphStageLogic(shape) with InHandler with OutHandler {

        private var queue: Queue[T]    = Queue.empty[T]
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
  final class AccumulateWhileUnchanged[E, P](propertyExtractor: E ⇒ P)
      extends GraphStage[FlowShape[E, immutable.Seq[E]]] {
    val in  = Inlet[E]("in")
    val out = Outlet[immutable.Seq[E]]("out")

    override def shape = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes) =
      new GraphStageLogic(shape) {
        private var currentState: Option[P] = None
        private val buffer                  = Vector.newBuilder[E]

        setHandlers(
          in,
          out,
          new InHandler with OutHandler {
            override def onPush(): Unit = {
              val nextElement = grab(in)
              val nextState   = propertyExtractor(nextElement)

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
          }
        )

        override def postStop(): Unit =
          buffer.clear()
      }
  }

  // Stage to measure backpressure
  //https://youtu.be/4s1YzgrRR2A?list=PLbZ2T3O9BuvczX5j03bWMrMFzK5OAs9mZ
  //https://github.com/naferx/akka-stream-checkpoint/blob/master/core/src/main/scala/akka/stream/checkpoint/CheckpointStage.scala
  final class BackpressureMeasurementStage[T] extends GraphStage[FlowShape[T, T]] {
    val in  = Inlet[T]("mes.in")
    val out = Outlet[T]("mes.out")

    override val shape: FlowShape[T, T] = FlowShape.of(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        //val histogram        = new org.HdrHistogram.Histogram(3600000000000L, 3)
        var lastPulled: Long = System.nanoTime
        var lastPushed: Long = lastPulled

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
            lastPulled = System.nanoTime
          }
        })

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              push(out, grab(in))
              val now            = System.nanoTime
              val lastPushedSpan = now - lastPushed

              val backpressureRatio =
                if (lastPushedSpan > 0) Some((lastPulled - lastPushed) * 100 / lastPushedSpan) else None

              //ratio between time spent for the pull and time spent for the push (percentage)
              // 0 percent means -- never backpressured
              // 100 percent means -- always backpressured
              //backpressureRatio.foreach(histogram.recordValue(_))
              lastPushed = now
              if (ThreadLocalRandom.current.nextDouble > .95)
                log.debug("{} BackpressureRatio: {}", Thread.currentThread.getName, backpressureRatio)
            }
          }
        )
        /*override def postStop(): Unit = {
          val out = new FileOutputStream(new File("./histograms/" + System.currentTimeMillis + ".txt"))
          val d   = (1000000.0).asInstanceOf[java.lang.Double]
          histogram.outputPercentileDistribution(new java.io.PrintStream(out), d) //in millis
        }*/
      }
  }

  /*
    Flow.fromGraph(new Chunker(chunkSize))
   */

  final class Chunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
    val in  = Inlet[ByteString]("chunker.in")
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
        }
      )

      private def emitChunk(): Unit =
        if (buffer.nonEmpty && isAvailable(out)) {
          val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
          buffer = nextBuffer
          push(out, chunk)
        } else {
          if (buffer.isEmpty)
            if (isClosed(in)) completeStage()
          if (!hasBeenPulled(in)) pull(in)
        }

      /*if (buffer.isEmpty) {
          if (isClosed(in)) completeStage() else pull(in)
        } else {
          if(isAvailable(out)) {
            val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
            buffer = nextBuffer
            push(out, chunk)
          }
        }*/
    }
  }

  //More interesting example here: https://github.com/calvinlfer/Akka-Streams-custom-stream-processing-examples/blob/master/src/main/scala/com/calvin/streamy/SideChannelSource.scala
  class QueueSrc[T](q: SinkQueueWithCancel[T]) extends GraphStage[SourceShape[T]] {
    val out: Outlet[T]                 = Outlet("queue-out")
    override val shape: SourceShape[T] = SourceShape(out)

    override protected def initialAttributes: Attributes =
      Attributes.name("queue-src") //.and(ActorAttributes.dispatcher(""))

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        var callback: AsyncCallback[Try[Option[T]]] = _
        var pending: Boolean                        = false
        val pendingBuffer                           = mutable.Queue[T]()

        override def preStart(): Unit = {
          callback = getAsyncCallback[Try[Option[T]]](onPullCompleted)
          q.pull().onComplete(callback.invoke)(materializer.executionContext)
        }

        def onPullCompleted(pullResult: Try[Option[T]]): Unit =
          pullResult match {
            case Success(Some(r)) ⇒
              pendingBuffer enqueue r
              if (pending) {
                val element = pendingBuffer.dequeue
                push(out, element)
                pending = false
              }
              q.pull().onComplete(callback.invoke)(materializer.executionContext)
            case Success(None) ⇒
              completeStage()
            case Failure(failure) ⇒
              failStage(failure)
          }

        def tryPush(): Unit =
          if (isAvailable(out) && pendingBuffer.nonEmpty) {
            val element = pendingBuffer.dequeue
            push(out, element)
          } else {
            pending = true
          }

        setHandler(out, new OutHandler {
          override def onPull(): Unit = tryPush()
        })
      }
  }

  /*
  Source.fromGraph(new PsJournal(...))  //in akka-pq
    .map(???)
    .viaMat(new LastSeen)(Keep.right)
   */
  class LastSeen[T] extends GraphStageWithMaterializedValue[FlowShape[T, T], Future[Option[T]]] {
    override val shape = FlowShape(Inlet[T]("in"), Outlet[T]("out"))

    override def createLogicAndMaterializedValue(
      inheritedAttributes: Attributes
    ): (GraphStageLogic, Future[Option[T]]) = {
      val matVal = Promise[Option[T]]
      val logic = new GraphStageLogic(shape) with StageLogging {

        import shape._

        private var current = Option.empty[T]

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val element = grab(in)
              current = Some(element)
              push(out, element)
            }

            override def onUpstreamFinish(): Unit = {
              log.info("upstream finish")
              matVal.success(current)
              super.onUpstreamFinish()
            }

            override def onUpstreamFailure(ex: Throwable): Unit = {
              log.info("upstream failure")
              matVal.success(current)

              //don't fail here intentionally
              //super.onUpstreamFailure(LastSeenException(ex, current))
              super.onUpstreamFinish()
            }
          }
        )

        setHandler(out, new OutHandler {
          override def onPull(): Unit = pull(in)
        })
      }
      (logic, matVal.future)
    }
  }

  /*
    Create a source where in order to get a number, you have to poll an API and the result is in a Future.
    The API could fail and we want to retry after 2 seconds.
   */
  import scala.concurrent.duration._
  final class AsyncSourceWithRetry(retryTo: FiniteDuration = 2.seconds) extends GraphStage[SourceShape[Int]] {
    val out: Outlet[Int] = Outlet[Int]("out")

    override val shape: SourceShape[Int] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with StageLogging {
        var callback: AsyncCallback[Int] = _
        var buffer: Vector[Int]          = Vector.empty
        var asyncCallInProgress          = false

        // grab the result of the asynchronous call and invoke the safe callback
        // also performs retries
        def grabAndInvokeWithRetry(future: Future[Int]): Unit = {
          asyncCallInProgress = true
          future.onComplete {
            case Success(randInt) ⇒
              callback.invoke(randInt)

            case Failure(ex) ⇒
              log.error(ex, "Error occurred in AsyncSourceWithRetry. Attempting again after 2 seconds")
              materializer.scheduleOnce(
                retryTo,
                () ⇒ grabAndInvokeWithRetry(pullRemoteApi(materializer.executionContext))
              )
          }(materializer.executionContext)
        }

        def pullRemoteApi(implicit ec: ExecutionContext) =
          Future {
            Thread.sleep(100)
            val next = scala.util.Random.nextInt(100)
            if (next < 10) throw new Exception("Number is below 10") with NoStackTrace else next
          }

        override def preStart(): Unit = {
          // Setup safe callback and its target handler

          // target handler of getAsyncCallback, this function will be called
          // when the side channel has data
          def bufferMessageAndEmulatePull(incoming: Int): Unit = {
            asyncCallInProgress = false
            buffer = buffer :+ incoming
            // emulate downstream asking for data by calling onPull on the outlet port
            // Note: we check whether the downstream is really asking there
            getHandler(out).onPull()
          }

          // In order to receive asynchronous events that are not arriving as stream elements
          // (for example a completion of a future or a callback from a 3rd party API) one must acquire a
          // AsyncCallback by calling getAsyncCallback() from the stage logic.
          // The method getAsyncCallback takes as a parameter a callback that will be called once the
          // asynchronous event fires.
          // This is a proxy that that asynchronous side channel needs to call for safety
          // this proxy delegates the data obtained from the callback to bufferMessageAndEmulatePull
          callback = getAsyncCallback[Int](bufferMessageAndEmulatePull)

          // emulate fake asynchronous call whose results we must obtain (which invokes the above callback)
          grabAndInvokeWithRetry(pullRemoteApi(materializer.executionContext))
        }

        setHandler(
          out,
          new OutHandler {
            // the downstream will pull us
            override def onPull(): Unit = {
              // we query here because bufferMessageAndEmulatePull artificially calls onPull
              // and we must not violate the GraphStages guarantees
              if (buffer.nonEmpty && isAvailable(out)) {
                val sendValue = buffer.head
                buffer = buffer.drop(1)
                push(out, sendValue)
              }

              // obtain more elements if the buffer is empty and if an asynchronous call already isn't in progress
              if (buffer.isEmpty && !asyncCallInProgress) {
                grabAndInvokeWithRetry(pullRemoteApi(materializer.executionContext))
              }
            }
          }
        )
      }
  }

}
