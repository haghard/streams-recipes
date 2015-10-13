package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel

import akka.actor._
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.{ ActorMaterializerSettings, OverflowStrategy, ActorMaterializer }
import akka.stream.actor.ActorSubscriberMessage.{ OnError, OnComplete, OnNext }
import akka.stream.actor._
import akka.stream.scaladsl._
import com.typesafe.config.ConfigFactory
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

//runMain recipes.AkkaRecipes
object AkkaRecipes extends App {

  val config = ConfigFactory.parseString(
    """
      |akka {
      |  flow-dispatcher {
      |    type = Dispatcher
      |    executor = "fork-join-executor"
      |    fork-join-executor {
      |      parallelism-min = 4
      |      parallelism-max = 8
      |    }
      |  }
      |  blocking-dispatcher {
      |    executor = "thread-pool-executor"
      |    thread-pool-executor {
      |      core-pool-size-min = 4
      |      core-pool-size-max = 4
      |    }
      |  }
      |}
    """.stripMargin)

  implicit val system = ActorSystem("Sys", ConfigFactory.empty().withFallback(config))
  val Settings = ActorMaterializerSettings(system)
    .withInputBuffer(32, 64)
    .withDispatcher("akka.flow-dispatcher")

  implicit val materializer = ActorMaterializer(Settings)

  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.134"), 8125)
  case class Tick()

  scenario3.run()

  /**
   * Fast publisher, Faster consumer
   * - publisher with a map to send, and a throttler
   * - Result: publisher and consumer rates should be equal.
   */
  def scenario1: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>

      import FlowGraph.Implicits._
      // get the elements for this flow.
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 20000, "fastProducer")
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingSyncActor], "fastSink", statsD))

      // connect source to sink
      source ~> fastSink
    }
  }

  /**
   * Fast publisher, fast consumer in the beginning get slower, no buffer
   * - same publisher as step 1.
   * - consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher and consumer will start at same rate. Publish rate will go down
   * together with publisher rate.
   *
   */
  def scenario2: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>
      import FlowGraph.Implicits._

      // get the elements for this flow.
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 10000, "fastProducer")
      val slowingSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownSink", statsD, 10l))

      // connect source to sink
      source ~> slowingSink
    }
  }

  /**
   * Fast publisher, fast consumer in the beginning get slower, with drop buffer
   * - same publisher as step 1.
   * - consumer, which gets slower (starts at no delay, increase delay with every message.
   * - Result: publisher stays at the same rate, consumer starts dropping messages
   */
  def scenario3: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 30 milliseconds, 10000, "fastProducer3")
      val slowingSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownSinkWithBuffer3", statsD, 20l))

      // now get the buffer, with 100 messages, which overflow
      // strategy that starts dropping messages when it is getting
      // too far behind.
      val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropHead)
      //val buffer = Flow[Int].buffer(1000, OverflowStrategy.backpressure)

      // connect source to sink with additional step
      source ~> buffer ~> slowingSink
    }
  }

  /**
   * Fast publisher, 2 fast consumers, one consumer which gets slower
   * - Result: publisher rate and all consumer rates go down at the same time
   */
  def scenario4: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>
      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 9000, "fastProducer")

      // and the sinks
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingSyncActor], "broadcast_fastsink", statsD, 0l))
      val slowingDownSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "broadcast_slowsink", statsD, 20l))

      // and the broadcast
      val broadcast = builder.add(Broadcast[Int](2))

      // use a broadcast to split the stream
      source ~> broadcast ~> fastSink
      broadcast ~> slowingDownSink
    }
  }

  /**
   * Fast publisher, 2 fast consumers, one consumer which gets slower but has buffer with drop
   * - Result: publisher rate and fast consumer rates stay the same. Slow consumer goes down.
   */
  def scenario5: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 20 milliseconds, 9000, "fastProducer5")

      // and the sinks
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingSyncActor], "fastSink5_0", statsD, 0l))
      val slowingDownSink1 = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowSink5_1", statsD, 5l))
      val slowingDownSink2 = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowSink5_2", statsD, 8l))

      //val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropTail)
      //val buffer = Flow[Int].buffer(128, OverflowStrategy.backpressure)

      val broadcast = builder.add(Broadcast[Int](3))

      // connect source to sink with additional step
      source ~> broadcast ~> fastSink
      broadcast ~> Flow[Int].buffer(2 << 6, OverflowStrategy.dropTail) ~> slowingDownSink1
      broadcast ~> Flow[Int].buffer(2 << 6, OverflowStrategy.dropTail) ~> slowingDownSink2
    }
  }

  /**
   * Fast publisher, 2 consumer which total 70msg/s, one gets slower with balancer
   * - Result: slowly more will be processed by fast one. When fast one can't keep up, publisher
   * will slow down*
   */
  def scenario6: RunnableGraph[Unit] = {
    FlowGraph.closed() { implicit builder =>

      import FlowGraph.Implicits._

      // first get the source
      val source = throttledSource(statsD, 1 second, 10 milliseconds, 500000, "fastProducer")

      // and the sin
      val fastSink = Sink.actorSubscriber(Props(classOf[DelayingSyncActor], "fastSinkWithBalancer", statsD, 12l))
      val slowingDownSink = Sink.actorSubscriber(Props(classOf[DegradingActor], "slowingDownWithBalancer", statsD, 14l, 1l))
      val balancer = builder.add(Balance[Int](2))

      // connect source to sink with additional step
      source ~> balancer ~> fastSink
      balancer ~> slowingDownSink
    }
  }

  /**
   * Merge[In] – (N inputs, 1 output) picks randomly from inputs pushing them one by one to its output
   * Several sources with different rates fan-in in single merge followed by sink
   * -Result: Sink rate = sum(sources)
   * @return
   */
  def scenario7: RunnableGraph[Unit] = {

    val queryStreams = Source() { implicit b ⇒
      import FlowGraph.Implicits._
      val streams = List("okcStream", "houStream", "miaStream", "sasStream")
      val latencies = List(20l, 30l, 40l, 45l).iterator
      val merge = b.add(Merge[Int](streams.size))
      streams.foreach { name ⇒
        Source.actorPublisher(Props(classOf[TopicReader], name, statsD, latencies.next())) ~> merge
      }
      merge.out
    }

    val fastSink = Sink.actorSubscriber(Props(classOf[DelayingSyncActor], "fastSink", statsD, 0l))

    //queryStreams.to(fastSink).run()

    FlowGraph.closed() { implicit builder ⇒
      import FlowGraph.Implicits._
      //val merge = builder.add(Merge[Int](4))

      //val okcSource = throttledSource(statsD, 1 second, 20 milliseconds, 10000, "okcSource")
      //val houSource = throttledSource(statsD, 1 second, 30 milliseconds, 10000, "houSource")
      //val miaSource = throttledSource(statsD, 1 second, 40 milliseconds, 10000, "miaSource")
      //val sasSource = throttledSource(statsD, 1 second, 45 milliseconds, 10000, "sasSource")

      //same
      //Source.actorPublisher(Props(classOf[TopicReader], "okcSource", statsD, 20l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "houSource", statsD, 30l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "miaSource", statsD, 40l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "sasSource", statsD, 45l)) ~> merge
      //merge ~> fastSink

      /*okcSource ~> merge
      houSource ~> merge
      miaSource ~> merge
      sasSource ~> merge
                   merge ~> fastSink*/

      queryStreams ~> fastSink
    }
  }

  import akka.stream.{ Attributes, UniformFanOutShape }
  import akka.stream.scaladsl.FlexiRoute.{ DemandFromAll, RouteLogic }

  /**
   *  Create custom Balance based on FlexiRoute with identical semantic to regular Balance junction
   */
  case class RoundRobinBalance[T](size: Int) extends FlexiRoute[T, UniformFanOutShape[T, T]](
    new UniformFanOutShape(size), Attributes.name(s"RoundRobinBalance-for-$size")) {
    private var cursor = -1
    private var index = 0
    override def createRouteLogic(s: UniformFanOutShape[T, T]) = new RouteLogic[T] {
      override def initialState = State[Unit](DemandFromAll((0 to (size - 1)).map(s.out(_)))) { (ctx, out, in) =>
        if (cursor == Int.MaxValue) cursor = -1
        cursor += 1
        index = cursor % size
        ctx.emit(s.out(index))(in)
        SameState
      }
      override def initialCompletionHandling = eagerClose
    }
  }

  /**
   * Execute nested flows in parallel and merge results
   */
  def scenario8: RunnableGraph[Unit] = {
    val parallelism = 4
    val bufferSize = 128
    val sink = Sink.actorSubscriber(Props(classOf[BatchActor], "splitMergeSink8", statsD, 0l, bufferSize))

    val latencies = List(10l, 30l, 35l, 45l).iterator

    val source = throttledSource(statsD, 1 second, 10 milliseconds, Int.MaxValue, "fastProducer8")

    def nested(sleep: Long) = Flow[Int].buffer(bufferSize, OverflowStrategy.backpressure).map { r => Thread.sleep(sleep); r }

    def bufAttrib = Attributes.inputBuffer(initial = bufferSize, max = bufferSize)

    FlowGraph.closed() { implicit b ⇒
      import FlowGraph.Implicits._
      //val balancer = b.add(RoundRobinBalance[Int](parallelism))
      val balancer = b.add(Balance[Int](parallelism).withAttributes(bufAttrib))
      val merge = b.add(Merge[Int](parallelism).withAttributes(bufAttrib))

      source ~> balancer

      for (i <- 0 until parallelism) {
        balancer ~> nested(latencies.next()) ~> merge
      }

      merge ~> sink
    }
  }

  /**
   * A Fast source with collapsed result and a constant sink.
   * No buffer is required
   *
   * Allow to progress top flow independently from bottom
   * using Conflate combinator
   */
  def scenario9: RunnableGraph[Unit] = {
    val sink = Sink.actorSubscriber(Props(classOf[DegradingActor], "timedSink9", statsD, 0l))

    val aggregatedSource = throttledSource(statsD, 1 second, 10 milliseconds, Int.MaxValue, "fastProducer9")
      .scan(State(0l, 0l)) { _ combine _ }
      .conflate(_.sum)(Keep.left)

    FlowGraph.closed() { implicit b ⇒
      import FlowGraph.Implicits._
      (aggregatedSource via throttledFlow(100 milliseconds)) ~> sink
    }
  }

  case class State(totalSamples: Long, sum: Long) {
    def combine(current: Long) = this.copy(this.totalSamples + 1, this.sum + current)
  }

  def every[T](interval: FiniteDuration): Flow[T, T, Unit] =
    Flow() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(ZipWith[T, Tick.type, T](Keep.left).withAttributes(Attributes.inputBuffer(1, 1)))
      val dropOne = b.add(Flow[T].drop(1))
      Source(Duration.Zero, interval, Tick) ~> zip.in1
      zip.out ~> dropOne.inlet
      (zip.in0, dropOne.outlet)
    }

  /**
   * Almost same as ``every``
   */
  def throttledFlow[T](interval: FiniteDuration): Flow[T, T, Unit] = {
    Flow() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(Zip[T, Tick]().withAttributes(Attributes.inputBuffer(1, 1)))
      Source(interval, interval, Tick()) ~> zip.in1
      (zip.in0, zip.out)
    }.map(_._1)
  }

  /**
   * Fast sink and heartbeats sink.
   * Sink's rate is equal to sum of 2 sources
   *
   */
  def scenario10: RunnableGraph[Unit] =
    FlowGraph.closed() { implicit b ⇒
      import FlowGraph.Implicits._
      val source = throttledSource(statsD, 1 second, 20 milliseconds, Int.MaxValue, "fastProducer10")
      val sink = Sink.actorSubscriber(Props(classOf[DegradingActor], "timedSink10", statsD, 0l))
      source ~> heartbeats(50.millis, 0) ~> sink
    }

  def heartbeats[T](interval: FiniteDuration, zero: T): Flow[T, T, Unit] =
    Flow() { implicit builder =>
      import FlowGraph.Implicits._
      val heartbeats = builder.add(Source(interval, interval, zero))
      val merge = builder.add(MergePreferred[T](1))
      heartbeats ~> merge.in(0)
      (merge.preferred, merge.out)
    }

  //Detached flows with conflate + conflate
  def scenario11: RunnableGraph[Unit] = {
    val srcSlow = throttledSource(statsD, 1 second, 1000 milliseconds, Int.MaxValue, "slowProducer11")
      .conflate(identity)(_ + _)

    val srcFast = throttledSource(statsD, 1 second, 200 milliseconds, Int.MaxValue, "fastProducer11")
      .conflate(identity)(_ + _)

    FlowGraph.closed() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(Props(classOf[DegradingActor], "sink11", statsD, 0l))
    }
  }

  //Detached flows with expand + conflate
  def scenario12: RunnableGraph[Unit] = {
    val srcSlow = throttledSource(statsD, 1 second, 1000 milliseconds, Int.MaxValue, "slowProducer12")
      .expand(identity)(r => (r + r, r))

    val srcFast = throttledSource(statsD, 1 second, 200 milliseconds, Int.MaxValue, "fastProducer12")
      .conflate(identity)(_ + _)

    FlowGraph.closed() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(Props(classOf[DegradingActor], "sink12", statsD, 0l))
    }
  }

  /**
   * Create a source which is throttled to a number of message per second.
   */
  def throttledSource(statsD: InetSocketAddress, delay: FiniteDuration, interval: FiniteDuration, numberOfMessages: Int, name: String): Source[Int, Unit] = {
    Source[Int]() { implicit b =>
      import FlowGraph.Implicits._
      val sendBuffer = ByteBuffer.allocate(1024)
      val channel = DatagramChannel.open()

      // two source
      val tickSource = Source(delay, interval, Tick())
      val rangeSource = Source(1 to numberOfMessages)

      def send(message: String) = {
        sendBuffer.put(message.getBytes("utf-8"))
        sendBuffer.flip()
        channel.send(sendBuffer, statsD)
        sendBuffer.limit(sendBuffer.capacity())
        sendBuffer.rewind()
      }

      val sendMap = b.add(Flow[Int] map { x => send(s"$name:1|c"); x })

      // we use zip to throttle the stream
      val zip = b.add(Zip[Tick, Int]())
      val unzip = b.add(Flow[(Tick, Int)].map(_._2))

      // setup the message flow
      tickSource ~> zip.in0
      rangeSource ~> zip.in1
      zip.out ~> unzip ~> sendMap

      sendMap.outlet
    }
  }
}

trait StatsD {
  val Encoding = "utf-8"

  val sendBuffer = ByteBuffer.allocate(1024)
  val channel = DatagramChannel.open()

  def address: InetSocketAddress

  def send(message: String) = {
    sendBuffer.put(message.getBytes("utf-8"))
    sendBuffer.flip()
    channel.send(sendBuffer, address)
    sendBuffer.limit(sendBuffer.capacity())
    sendBuffer.rewind()
  }
}

/**
 * Same is throttledSource
 * @param name
 * @param address
 * @param delay
 */
class TopicReader(name: String, val address: InetSocketAddress, delay: Long) extends ActorPublisher[Int] with StatsD {
  val Limit = 10000
  var progress = 0
  val observeGap = 1000

  override def receive: Actor.Receive = {
    case Request(n) => if (isActive && totalDemand > 0) {
      var n0 = n

      if (progress >= Limit)
        self ! Cancel

      while (n0 > 0) {
        if (progress % observeGap == 0)
          println(s"$name: $progress")

        progress += 1
        onNext(progress)
        Thread.sleep(delay)
        send(s"$name:1|c")
        n0 -= 1
      }
    }

    case Cancel =>
      println(name + " is canceled")
      context.system.stop(self)
  }
}

class DelayingActor2(name: String, val address: InetSocketAddress, delay: Long) extends ActorSubscriber with ActorPublisher[Long] with StatsD {

  val queue = mutable.Queue[Long]()

  override protected val requestStrategy: RequestStrategy = new MaxInFlightRequestStrategy(10) {
    override val inFlightInternally = queue.size
  }

  def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) =>
      Thread.sleep(delay)
      send(s"$name:1|c")
      queue += msg
      tryReply

    case OnComplete =>
      println(s"Complete DelayingActor2")
      context.system.stop(self)

    case OnError(ex) ⇒
      onError(ex)
      println("OnError {}", ex.getMessage)

    case Request(n) ⇒
      println(n)
      tryReply()

    case Cancel ⇒
      cancel()
      println("Cancel")
  }

  def tryReply() = {
    while ((isActive && totalDemand > 0) && !queue.isEmpty) {
      onNext(queue.dequeue())
    }
  }
}

class DelayingSyncActor(name: String, val address: InetSocketAddress, delay: Long) extends ActorSubscriber with StatsD {

  override protected val requestStrategy: RequestStrategy = OneByOneRequestStrategy

  def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) =>
      //println(s"Sink $msg")
      //Thread.sleep(delay)
      send(s"$name:1|c")

    case OnComplete =>
      println(s"Complete DelayingActor")
      context.system.stop(self)
  }
}

class BatchActor(name: String, val address: InetSocketAddress, delay: Long, bufferSize: Int) extends ActorSubscriber with StatsD {

  private val queue = new mutable.Queue[Int]()

  override protected val requestStrategy = new MaxInFlightRequestStrategy(bufferSize) {
    override val inFlightInternally = queue.size
  }

  def this(name: String, statsD: InetSocketAddress, bufferSize: Int) {
    this(name, statsD, 0, bufferSize)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) =>
      if (queue.size == bufferSize) flush()
      else queue += msg

    case OnComplete =>
      println(s"Complete DelayingActor")
      context.system.stop(self)
  }

  private def flush() = {
    while (!queue.isEmpty) {
      val _ = queue.dequeue()
      send(s"$name:1|c")
    }
  }
}

class DegradingActor(val name: String, val address: InetSocketAddress, delayPerMsg: Long, initialDelay: Long) extends ActorSubscriber with StatsD {

  override protected val requestStrategy: RequestStrategy = OneByOneRequestStrategy

  // default delay is 0
  var delay = 0l

  def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0)
  }

  def this(name: String, statsD: InetSocketAddress, delayPerMsg: Long) {
    this(name, statsD, delayPerMsg, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) =>
      delay += delayPerMsg
      Thread.sleep(initialDelay + (delay / 1000), delay % 1000 toInt)
      send(s"$name:1|c")

    case OnNext(msg: (Int, Int)) =>
      println(msg)
      send(s"$name:1|c")

    case OnComplete =>
      println(s"Complete DegradingActor")
      context.system.stop(self)
  }
}