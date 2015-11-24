package recipes

import java.net.{ InetAddress, InetSocketAddress }
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.routing.{ ActorRefRoutee, RoundRobinRoutingLogic, Router }
import akka.stream._
import akka.stream.actor.ActorPublisherMessage.{ Cancel, Request }
import akka.stream.actor.ActorSubscriberMessage.{ OnComplete, OnError, OnNext }
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import com.typesafe.config.ConfigFactory
import recipes.BatchProducer.Item
import recipes.WorkerRouter.DBObject

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.language.postfixOps
import scala.util.{ Failure, Success }
import scalaz.{ \/-, -\/, \/ }

//runMain recipes.AkkaRecipes
object AkkaRecipes extends App {

  val config = ConfigFactory.parseString(
    """
      |akka {
      |  flow-dispatcher {
      |    type = Dispatcher
      |    executor = "fork-join-executor"
      |    fork-join-executor {
      |      parallelism-min = 8
      |      parallelism-max = 16
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

  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.134"), 8125)

  implicit val sys: ActorSystem = ActorSystem("Sys", ConfigFactory.empty().withFallback(config))

  val decider: akka.stream.Supervision.Decider = {
    case ex: Throwable ⇒
      println(ex.getMessage)
      akka.stream.Supervision.Stop
  }

  val Settings = ActorMaterializerSettings.create(system = sys)
    .withInputBuffer(32, 32)
    .withSupervisionStrategy(decider)
    .withDispatcher("akka.flow-dispatcher")

  implicit val Mat = ActorMaterializer(Settings)

  val Mat0 = ActorMaterializer(ActorMaterializerSettings.create(system = sys)
    .withInputBuffer(1, 1)
    .withSupervisionStrategy(decider)
    .withDispatcher("akka.flow-dispatcher"))

  RunnableGraph.fromGraph(scenario11).run()(Mat0)

  /**
   *
   */
  def consoleProgress(name: String, duration: FiniteDuration) =
    (Flow[Int].conflate(_ ⇒ 0)((c, _) ⇒ c + 1)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left))
      .scan(0)(_ + _)
      .to(Sink.foreach(c ⇒ println(s"$name: $c")))

  /**
   * Fast publisher and consumer
   * Result: publisher and consumer stay on the same rate.
   */
  def scenario1: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val fastSource = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source1")
      val fastSink = Sink.actorSubscriber(SyncActor.props2("akka-sink1", statsD))

      (fastSource alsoTo consoleProgress("akka-scenario1", 5 seconds)) ~> fastSink
      ClosedShape
    }
  }

  /**
   * Fast publisher and fast consumer in the beginning, consumer gets slower, increase delay with every message.
   * We use buffer with OverflowStrategy.backpressure in between which makes producer slower
   * Result: Publisher and consumer should start at same rate.
   * Publisher and consumer rate should decrease proportionally later.
   */
  def scenario2: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val fastSource = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source2")
      val degradingSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink2", statsD, 1l))
      val buffer = Flow[Int].buffer(1 << 7, OverflowStrategy.backpressure)
      (fastSource alsoTo consoleProgress("akka-scenario2", 5 seconds)) ~> buffer ~> degradingSink
      ClosedShape
    }
  }

  /**
   * Fast publisher and fast consumer in the beginning,
   * consumer gets slower, increase delay with every message.
   * We use buffer with OverflowStrategy.dropHead in between which leads to dropping oldest messages
   * Result: publisher stays at the original rate and starts drop oldest messages, consumer is getting slower
   */
  def scenario3: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source3")
      val slowingSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink3", statsD, 1l))
      //OverflowStrategy.dropHead will drop the oldest waiting job
      //OverflowStrategy.dropTail will drop the youngest waiting job
      val buffer = Flow[Int].buffer(1 << 7, OverflowStrategy.dropHead)
      (source alsoTo consoleProgress("akka-scenario3", 5 seconds)) ~> buffer ~> slowingSink
      ClosedShape
    }
  }

  /**
   * Fast publisher, 2 fast consumers, one consumer gets slower over time
   * Result: publisher rate and all consumer rates go down at the same time
   */
  def scenario4: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source4")
      val fastSink = Sink.actorSubscriber(SyncActor.props("akka-sink4_0", statsD, 0l))
      val slowSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink4_1", statsD, 2l))
      val broadcast = builder.add(Broadcast[Int](2))

      (source alsoTo consoleProgress("akka-scenario4", 5 seconds)) ~> broadcast ~> fastSink
      broadcast ~> slowSink
      ClosedShape
    }
  }

  /**
   * Fast publisher ans 3 sinks, 1 fast and 2 degrading. Degrading consumers are
   * getting messages through buffer with OverflowStrategy.dropTail strategy
   *
   * Result: publisher rate and fast consumer rates stay the same.
   * Degrading consumers rate goes down but doesn't affect the whole flow.
   */
  def scenario5: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, 20000, "akka-source5")

      val fastSink = Sink.actorSubscriber(SyncActor.props("akka-sink5_0", statsD, 0l))
      val degradingSink1 = Sink.actorSubscriber(DegradingActor.props2("akka-sink5_1", statsD, 8l))
      val degradingSink2 = Sink.actorSubscriber(DegradingActor.props2("akka-sink5_2", statsD, 10l))

      //val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropTail)
      //val buffer = Flow[Int].buffer(128, OverflowStrategy.backpressure)

      val broadcast = builder.add(Broadcast[Int](3))

      // connect source to sink with additional step
      source ~> broadcast ~> fastSink
      broadcast ~> Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) ~> degradingSink1
      broadcast ~> Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) ~> degradingSink2
      ClosedShape
    }
  }

  def scenario6: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, 20000, "akka-source6")

      val fastSink = Sink.actorSubscriber(SyncActor.props("akka-sink6_0", statsD, 12l))
      val slowingDownSink = Sink.actorSubscriber(DegradingActor.props("akka-sink6_1", statsD, 14l, 1l))
      val balancer = builder.add(Balance[Int](2))

      source ~> balancer ~> fastSink
      balancer ~> slowingDownSink
      ClosedShape
    }
  }

  /**
   * Merge[In] – (N inputs, 1 output) picks randomly from inputs pushing them one by one to its output
   * Several sources with different rates fan-in in single merge followed by sink
   * -Result: Sink rate = sum(sources)
   * @return
   */
  def scenario7: Graph[ClosedShape, Unit] = {
    val latencies = List(20l, 30l, 40l, 45l).iterator
    val srcs = List("akka-source7_0", "akka-source7_1", "akka-source7_2", "akka-source7_3")

    lazy val sources = srcs.map { name ⇒
      Source.actorPublisher[Int](Props(classOf[TopicReader], name, statsD, latencies.next()).withDispatcher("akka.flow-dispatcher"))
    }

    lazy val multiSource = Source.fromGraph(
      FlowGraph.create() { implicit b ⇒
        import FlowGraph.Implicits._
        val merger = b.add(Merge[Int](sources.size))
        sources.zipWithIndex.foreach {
          case (src, idx) ⇒ b.add(src) ~> merger.in(idx)
        }
        SourceShape(merger.out)
      }
    )

    val queryStreams = Source.fromGraph(
      FlowGraph.create() { implicit b ⇒
        import FlowGraph.Implicits._
        val merge = b.add(Merge[Int](srcs.size))
        srcs.foreach { name ⇒
          Source.actorPublisher(Props(classOf[TopicReader], name, statsD, latencies.next())
            .withDispatcher("akka.flow-dispatcher")) ~> merge
        }
        SourceShape(merge.out)
      }
    )

    FlowGraph.create() { implicit builder ⇒
      import FlowGraph.Implicits._
      //val merge = builder.add(Merge[Int](4))

      //val okcSource = throttledSource(statsD, 1 second, 20 milliseconds, 10000, "firstSrc7_1")
      //val houSource = throttledSource(statsD, 1 second, 30 milliseconds, 10000, "secondSrc7_2")
      //val miaSource = throttledSource(statsD, 1 second, 40 milliseconds, 10000, "thirdSrc7_3")
      //val sasSource = throttledSource(statsD, 1 second, 45 milliseconds, 10000, "fourthSrc7_4")

      //same
      //Source.actorPublisher(Props(classOf[TopicReader], "firstSrc7_1", statsD, 20l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "secondSrc7_2", statsD, 30l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "thirdSrc7_3", statsD, 40l)) ~> merge
      //Source.actorPublisher(Props(classOf[TopicReader], "fourthSrc7_4", statsD, 45l)) ~> merge
      //merge ~> fastSink

      //multiSource ~> fastSink
      (queryStreams alsoTo consoleProgress("akka-scenario7", 5 seconds)) ~> Sink.actorSubscriber(SyncActor.props("akka-sink7", statsD, 0l))
      ClosedShape
    }
  }

  final class DisjunctionRouterStage[T, A](validationLogic: T ⇒ A \/ T) extends GraphStage[FanOutShape2[T, A, T]] {
    val in = Inlet[T]("in")
    val error = Outlet[A]("error")
    val out = Outlet[T]("out")

    override def shape = new FanOutShape2[T, A, T](in, error, out)

    override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
      var initialized = false
      var pending: Option[(A, Outlet[A]) \/ (T, Outlet[T])] = None

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          pending = validationLogic(grab(in)).fold(
            { err: A ⇒ Option(-\/(err, error)) },
            { v: T ⇒ Option(\/-(v, out)) }
          )
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
        pending.foreach { pen ⇒
          pen.fold({ kv ⇒ tryPushError(kv._1, kv._2) }, { kv ⇒ tryPush(kv._1, kv._2) })
        }
      }
    }
  }

  final class RoundRobinStage4[T] extends GraphStage[FanOutShape4[T, T, T, T, T]] {
    val in = Inlet[T]("in")
    val outlets = Vector(Outlet[T]("out0"), Outlet[T]("out1"), Outlet[T]("out2"), Outlet[T]("out3"))
    var seqNum = 0
    val s = outlets.size

    override def shape = new FanOutShape4[T, T, T, T, T](in, outlets(0), outlets(1), outlets(2), outlets(3))

    override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
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

  /**
   * Execute nested flows in PARALLEL and merge results
   * Parallel fan-out fan-in
   */
  def scenario8: Graph[ClosedShape, Unit] = {
    val parallelism = 4
    val bufferSize = 128

    val sink = Sink.actorSubscriber(SyncActor.props("akka-sink8", statsD, 0l))

    val latencies = List(20l, 30l, 40l, 45l)
    val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source8")

    def parAction(sleep: Long) = Flow[Int].buffer(bufferSize, OverflowStrategy.backpressure)
      .map { r ⇒ Thread.sleep(sleep); r }

    def buffAttributes = Attributes.inputBuffer(initial = bufferSize, max = bufferSize)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      //val balancer = b.add(new RoundRobinStage4[Int].withAttributes(buffAttributes))
      val balancer = b.add(Balance[Int](parallelism))
      val merge = b.add(Merge[Int](parallelism).withAttributes(buffAttributes))

      (source alsoTo consoleProgress("akka-scenario8", 5 seconds)) ~> balancer

      latencies.zipWithIndex.foreach {
        case (l, ind) ⇒
          balancer.outArray(ind) ~> parAction(l) ~> merge
      }

      //balancer.out0 ~> parAction(latencies(0)) ~> merge
      //balancer.out1 ~> parAction(latencies(1)) ~> merge
      //balancer.out2 ~> parAction(latencies(2)) ~> merge
      //balancer.out3 ~> parAction(latencies(3)) ~> merge
      merge ~> sink
      ClosedShape
    }
  }

  /**
   *
   *
   */
  def scenario08: Graph[ClosedShape, Unit] = {
    val source = throttledSrc(statsD, 1 second, 100 milliseconds, Int.MaxValue, "akka-source08")
    val errorSink = Sink.actorSubscriber(SyncActor.props("akka-sink-error08", statsD, 0l)) //slow sink
    val sink = Sink.actorSubscriber(SyncActor.props("akka-sink08", statsD, 0l))

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      val balancer = b.add(new DisjunctionRouterStage[Int, String]({ el: Int ⇒
        if (el % 10 == 0) -\/(s"error element $el") else \/-(el)
      }))

      source ~> balancer.in
      balancer.out0 ~> Flow[String].buffer(64, OverflowStrategy.dropHead) ~> errorSink
      balancer.out1 ~> sink
      ClosedShape
    }
  }

  /**
   * A Fast source with conflate flow that buffer incoming message and produce single element
   */
  def scenario9: Graph[ClosedShape, Unit] = {
    val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source9")
    val sink = Sink.actorSubscriber(DegradingActor.props2("akka-sink9", statsD, 0l))

    //conflate as buffer but without backpressure support
    def conflate0: Flow[Int, Int, Unit] =
      Flow[Int].conflate(Vector(_))((acc, element) ⇒ acc :+ element)
        .mapConcat(identity)

    def buffer = Flow[Int].buffer(128, OverflowStrategy.backpressure)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      ((source via buffer) via throttledFlow(100 milliseconds)) ~> sink
      ClosedShape
    }
  }

  /**
   * A Fast source with collapsed result and a fast sink.
   * No buffer is required
   *
   * Allow to progress top flow independently from bottom
   * using Conflate combinator
   */
  def scenario9_1: Graph[ClosedShape, Unit] = {
    val sink = Sink.actorSubscriber(DegradingActor.props2("akka-source9_1", statsD, 0l))

    val aggregatedSource = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-sink9_1")
      .scan(State(0l, 0l)) {
        _ combine _
      }
      .conflate(_.sum)(Keep.left)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      (aggregatedSource via throttledFlow(500 milliseconds)) ~> sink
      ClosedShape
    }
  }

  case class State(totalSamples: Long, sum: Long) {
    def combine(current: Long) = this.copy(this.totalSamples + 1, this.sum + current)
  }

  def every[T](interval: FiniteDuration): Flow[T, T, Unit] =
    Flow.fromGraph(
      FlowGraph.create() { implicit b ⇒
        import FlowGraph.Implicits._
        val zip = b.add(ZipWith[T, Unit, T](Keep.left).withAttributes(Attributes.inputBuffer(1, 1)))
        val dropOne = b.add(Flow[T].drop(1))
        Source.tick(Duration.Zero, interval, ()) ~> zip.in1
        zip.out ~> dropOne.inlet
        FlowShape(zip.in0, dropOne.outlet)
      }
    )

  /**
   * Almost same as ``every``
   */
  def throttledFlow[T](interval: FiniteDuration): Flow[T, T, Unit] = {
    Flow.fromGraph(
      FlowGraph.create() { implicit b ⇒
        import FlowGraph.Implicits._
        val zip = b.add(Zip[T, Unit]().withAttributes(Attributes.inputBuffer(1, 1)))
        Source.tick(interval, interval, ()) ~> zip.in1
        FlowShape(zip.in0, zip.out)
      }
    ).map(_._1)
  }

  /**
   * Fast sink and heartbeats sink.
   * Sink's rate is equal to sum of 2 sources
   *
   */
  def scenario10: Graph[ClosedShape, Unit] =
    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      val source = throttledSrc(statsD, 1 second, 20 milliseconds, Int.MaxValue, "akka-source10")
      val sink = Sink.actorSubscriber(DegradingActor.props2("akka-sink10", statsD, 0l))
      source ~> heartbeats(50.millis, 0) ~> sink
      ClosedShape
    }

  def heartbeats[T](interval: FiniteDuration, zero: T): Flow[T, T, Unit] =
    Flow.fromGraph(
      FlowGraph.create() { implicit builder ⇒
        import FlowGraph.Implicits._
        val heartbeats = builder.add(Source.tick(interval, interval, zero))
        val merge = builder.add(MergePreferred[T](1))
        heartbeats ~> merge.in(0)
        FlowShape(merge.preferred, merge.out)
      }
    )

  //Detached flows with conflate + conflate
  def scenario11: Graph[ClosedShape, Unit] = {
    val srcSlow = throttledSrc(statsD, 1 second, 1000 milliseconds, Int.MaxValue, "akka-source11_0")
      .conflate(identity)(_ + _)

    val srcFast = throttledSrc(statsD, 1 second, 200 milliseconds, Int.MaxValue, "akka-source11_1")
      .conflate(identity)(_ + _)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink11", statsD, 0l))
      ClosedShape
    }
  }

  //Detached flows with expand + conflate
  def scenario12: Graph[ClosedShape, Unit] = {
    val srcFast = throttledSrc(statsD, 1 second, 200 milliseconds, Int.MaxValue,
      "akka-source12_1").conflate(identity)(_ + _)

    val srcSlow = throttledSrc(statsD, 1 second, 1000 milliseconds, Int.MaxValue,
      "akka-source12_0").expand(identity) { r ⇒ (r + r, r) }

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink12", statsD, 0l))
      ClosedShape
    }
  }

  /**
   * External source
   * In 2.0 for this purpose you should can use  [[Source.queue.offer]]
   */
  def scenario13: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      //no backpressure there, just dropping
      val (actor, publisher) = Source.actorRef[Int](200, OverflowStrategy.dropTail)
        .toMat(Sink.publisher[Int](false))(Keep.both).run()

      val i = new AtomicInteger()
      sys.scheduler.schedule(1 second, 20 milliseconds) {
        i.getAndIncrement()
        if (i.get() < Int.MaxValue) actor ! i
      }(sys.dispatchers.lookup("akka.flow-dispatcher"))

      Source(publisher) ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink13", statsD, 13l))
      ClosedShape
    }
  }

  /**
   * External Producer through Source.queue
   */
  def scenario13_1: Graph[ClosedShape, Unit] = {
    implicit val Ctx = sys.dispatchers.lookup("akka.flow-dispatcher")
    implicit val ExtCtx = sys.dispatchers.lookup("akka.blocking-dispatcher")

    val pubStatsD = new StatsD {
      override val address = statsD
    }
    val (queue, publisher) = Source.queue[Option[Int]](1 << 7, OverflowStrategy.backpressure)
      .takeWhile(_.isDefined).map(_.get)
      .toMat(Sink.publisher[Int](false))(Keep.both).run()

    def externalProducer(q: SourceQueue[Option[Int]], pName: String, i: Int): Unit = {
      if (i < Int.MaxValue)
        (q.offer(Option(i))).onComplete {
          _ match {
            case Success(r) ⇒
              (pubStatsD send pName)
              externalProducer(q, pName, i + 1)
            case Failure(ex) ⇒
              println(ex.getMessage)
              sys.scheduler.scheduleOnce(1 seconds)(externalProducer(q, pName, i))(ExtCtx) //retry
          }
        }(ExtCtx)
      else q.offer(None).onComplete(_ ⇒ (pubStatsD send pName))(ExtCtx)
    }

    externalProducer(queue, "source_13_1:1|c", 0)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      Source(publisher) ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink13", statsD, 1l))
      ClosedShape
    }
  }

  /**
   * Batched source with external effect as an Actor through Flow and degrading sink
   * The whole pipeline is going to slow down up to sink's rate
   * http://fehmicansaglam.net/connecting-dots-with-akka-stream/
   *
   * In this scenario let us assume that we are reading a bulk of items from an internal system,
   * making a request for each item to an external service,
   * then sending an event to a stream (e.g. Kafka) or don't sent
   * for each item received from the external service.
   * Each stage should support non-blocking back pressure.
   */
  def scenario14: Graph[ClosedShape, Unit] = {
    val batchedSource = Source.actorPublisher[Vector[Item]](BatchProducer.props)
    val sink = Sink.actorSubscriber[Int](DegradingActor.props2("akka-sink14", statsD, 10l))
    val external = Flow[Item].buffer(1, OverflowStrategy.backpressure).map(r ⇒ r.num)

    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      /*
      val parallelism = 4
      val externalRequestFlow = Flow[String].mapAsyncUnordered(parallelism) { query =>
        Http().singleRequest {
          HttpRequest(uri = Uri(...).withQuery("query" -> query))
        }
      }.mapAsyncUnordered(parallelism) { response =>
        //Unmarshal(response.entity).to[ExternalItem]
      }.withAttributes(supervisionStrategy(resumingDecider))
      */

      (batchedSource mapConcat identity) ~> external ~> sink
      ClosedShape
    }
  }

  /**
   * Router pulls from the DbCursorPublisher and runs parallel processing for records
   * Router dictates rate to publisher
   * Parallel
   * +------+
   * +--|Worker|--+
   * |  +------+  |
   * +-----------------+     +------------+        |  +------+  |  +-----------+
   * |DbCursorPublisher|-----|WorkerRouter|--------|--|Worker|-----|RecordsSink|
   * +-----------------+     +------------+        |  +------+  |  +-----------+
   * |  +------+  |
   * +--|Worker|--+
   * +------+
   */
  def scenario15: Graph[ClosedShape, Unit] = {
    FlowGraph.create() { implicit b ⇒
      import FlowGraph.Implicits._
      val out = sys.actorOf(Props(classOf[RecordsSink], "sink15", statsD).withDispatcher("akka.flow-dispatcher"), "akka-sink15")
      val src = Source.actorPublisher[Long](Props(classOf[DbCursorPublisher], "akka-source15", 20000l, statsD).withDispatcher("akka.flow-dispatcher")).map(r ⇒ DBObject(r, out))
      src ~> Sink.actorSubscriber(WorkerRouter.props)
      ClosedShape
    }
  }

  /**
   * Create a source which is throttled to a number of message per second.
   */
  def throttledSrc(statsD: InetSocketAddress, delay: FiniteDuration, interval: FiniteDuration, limit: Int, name: String): Source[Int, Unit] = {
    Source.fromGraph(
      FlowGraph.create() { implicit b ⇒
        import FlowGraph.Implicits._
        val sendBuffer = ByteBuffer.allocate(1024)
        val channel = DatagramChannel.open()

        // two source
        val tickSource = Source.tick(delay, interval, ())
        val rangeSource = Source(() ⇒ Iterator.range(1, limit))

        def send(message: String) = {
          sendBuffer.put(message getBytes "utf-8")
          sendBuffer.flip()
          channel.send(sendBuffer, statsD)
          sendBuffer.limit(sendBuffer.capacity())
          sendBuffer.rewind()
        }

        val sendMap = b.add(Flow[Int] map { x ⇒ send(s"$name:1|c"); x })

        // we use zip to throttle the stream
        val zip = b.add(Zip[Unit, Int]())
        val unzip = b.add(Flow[(Unit, Int)].map(_._2))

        // setup the message flow
        tickSource ~> zip.in0
        rangeSource ~> zip.in1
        zip.out ~> unzip ~> sendMap

        SourceShape(sendMap.outlet)
      }
    )
  }
}

trait StatsD {
  val Encoding = "utf-8"
  val sendBuffer = (ByteBuffer allocate 1024)
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

object WorkerRouter {

  case class DBObject(id: Long, replyTo: ActorRef)

  case class Work(id: Long)

  case class Reply(id: Long)

  case class Done(id: Long)

  def props: Props = Props(new WorkerRouter).withDispatcher("akka.flow-dispatcher")
}

class WorkerRouter extends ActorSubscriber with ActorLogging {

  import WorkerRouter._

  val MaxInFlight = 32
  var requestors = Map.empty[Long, ActorRef]
  val n = Runtime.getRuntime.availableProcessors() / 2

  val workers = (1 to n)
    .map(i ⇒ s"worker-$i")
    .map(name ⇒ context.actorOf(Props(classOf[Worker], name).withDispatcher("akka.flow-dispatcher"), name))

  var router = Router(RoundRobinRoutingLogic(),
    workers.map { r ⇒
      (context watch r)
      ActorRefRoutee(r)
    }
  )

  override val requestStrategy = new MaxInFlightRequestStrategy(MaxInFlight) {
    override def inFlightInternally = requestors.size
  }

  override def receive = {
    case Terminated(routee) ⇒
      router = (router removeRoutee routee)
      /*
      if we needed recreate route we would do this
      val resurrected = context.actorOf(Props[Worker].withDispatcher("akka.flow-dispatcher"))
      (context watch resurrected)
      (router addRoutee resurrected)
      */

      if (router.routees.size == 0) {
        log.info("All routees have been stopped")
        (context stop self)
      }

    case OnNext(DBObject(id, requestor)) ⇒
      requestors += (id -> requestor)
      (router route (Work(id), self))
    case Reply(id) ⇒
      requestors(id) ! Done(id)
      requestors -= id
    case OnComplete ⇒
      log.info("worker-router has received OnComplete")
      workers.foreach { r ⇒ (context stop r) }

    case OnError(ex) ⇒ log.info("OnError {}", ex.getMessage)
  }
}

class Worker(name: String) extends Actor with ActorLogging {

  import WorkerRouter._

  override def receive = {
    case Work(id) ⇒
      //log.info("{} got a job {}", name, id)
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 150))
      sender() ! Reply(id)
  }
}

class RecordsSink(name: String, val address: InetSocketAddress) extends Actor with ActorLogging with StatsD {
  override def receive = {
    case WorkerRouter.Done(id) ⇒
      send(s"$name:1|c")
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
    case Request(n) ⇒ if (isActive && totalDemand > 0) {
      var n0 = n

      if (progress >= Limit)
        self ! Cancel

      while (n0 > 0) {
        progress += 1
        onNext(progress)
        Thread.sleep(delay)
        send(s"$name:1|c")
        n0 -= 1
      }
    }

    case Cancel ⇒
      println(s"$name is canceled")
      context.system.stop(self)
  }
}

object PubSubSink {
  def props(name: String, address: InetSocketAddress, delay: Long) =
    Props(new PubSubSink(name, address, delay)).withDispatcher("akka.flow-dispatcher")

  def props2(name: String, address: InetSocketAddress) =
    Props(new PubSubSink(name, address)).withDispatcher("akka.flow-dispatcher")
}

class PubSubSink private (name: String, val address: InetSocketAddress, delay: Long) extends ActorSubscriber with ActorPublisher[Long] with StatsD {
  private val queue = mutable.Queue[Long]()

  override protected val requestStrategy = new MaxInFlightRequestStrategy(10) {
    override def inFlightInternally = queue.size
  }

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      Thread.sleep(delay)
      send(s"$name:1|c")
      queue += msg
      reply

    case OnComplete ⇒
      println("PubSubSink OnComplete")
      (context stop self)

    case OnError(ex) ⇒
      onError(ex)
      println("OnError {}", ex.getMessage)

    case Request(n) ⇒
      println(n)
      reply

    case Cancel ⇒
      cancel()
      println("Cancel")
  }

  private def reply = {
    while ((isActive && totalDemand > 0) && !queue.isEmpty) {
      send(s"$name:1|c")
      onNext(queue.dequeue)
    }
  }
}

object SyncActor {
  def props(name: String, address: InetSocketAddress, delay: Long) =
    Props(new SyncActor(name, address, delay)).withDispatcher("akka.flow-dispatcher")

  def props2(name: String, address: InetSocketAddress) =
    Props(new SyncActor(name, address)).withDispatcher("akka.flow-dispatcher")

  def props3(name: String, address: InetSocketAddress, delay: Long) =
    Props(new SyncActor(name, address, delay))
}

class SyncActor private (name: String, val address: InetSocketAddress, delay: Long) extends ActorSubscriber with StatsD {
  override protected val requestStrategy = OneByOneRequestStrategy

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      send(s"$name:1|c")

    case OnNext(msg: String) ⇒
      send(s"$name:1|c") //error
      Thread.sleep(3000)

    case OnComplete ⇒
      println(s"Complete DelayingActor")
      context.system.stop(self)
  }
}

object BatchActor {
  def props(name: String, address: InetSocketAddress, delay: Long, bufferSize: Int) =
    Props(new BatchActor(name, address, delay, bufferSize)).withDispatcher("akka.flow-dispatcher")
}

class BatchActor private (name: String, val address: InetSocketAddress, delay: Long, bufferSize: Int) extends ActorSubscriber with StatsD {
  private val queue = new mutable.Queue[Int]()

  override protected val requestStrategy = new MaxInFlightRequestStrategy(bufferSize) {
    override def inFlightInternally = queue.size
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      queue += msg
      if (queue.size >= bufferSize) flush()

    case OnComplete ⇒
      println(s"Complete BatchActor")
      context.system.stop(self)
    case OnError(ex) ⇒
      println(s"OnError BatchActor: ${ex.getMessage}")
      context.system.stop(self)
  }

  private def flush() = {
    while (!queue.isEmpty) {
      val _ = queue.dequeue
      send(s"$name:1|c")
    }
  }
}

object DegradingActor {
  def props(name: String, address: InetSocketAddress, delayPerMsg: Long, initialDelay: Long) =
    Props(new DegradingActor(name, address, delayPerMsg, initialDelay)).withDispatcher("akka.flow-dispatcher")

  def props2(name: String, address: InetSocketAddress, delayPerMsg: Long) =
    Props(new DegradingActor(name, address, delayPerMsg)).withDispatcher("akka.flow-dispatcher")
}

class DegradingActor private (val name: String, val address: InetSocketAddress, delayPerMsg: Long, initialDelay: Long) extends ActorSubscriber with StatsD {
  override protected val requestStrategy: RequestStrategy = OneByOneRequestStrategy
  var delay = 0l

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0)
  }

  private def this(name: String, statsD: InetSocketAddress, delayPerMsg: Long) {
    this(name, statsD, delayPerMsg, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      delay += delayPerMsg
      val latency = initialDelay + (delay / 1000)
      Thread.sleep(latency, (delay % 1000).toInt)
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: Long) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: (Int, Int)) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnComplete ⇒
      println(s"Complete DegradingActor")
      (context stop self)
  }
}

class DbCursorPublisher(name: String, val end: Long, val address: InetSocketAddress) extends ActorPublisher[Long] with StatsD with ActorLogging {
  var limit = 0l
  var seqN = 0l
  val showPeriod = 50

  override def receive: Receive = {
    case Request(n) if (isActive && totalDemand > 0) ⇒
      if (seqN >= end)
        onCompleteThenStop()

      limit = n
      while (limit > 0) {
        seqN += 1
        limit -= 1
        //log.info("fetch {}", seqN)
        if (limit % showPeriod == 0) {
          log.info("Cursor {}", seqN)
          Thread.sleep(200) //cursor buffer
        }
        onNext(seqN)
        send(s"$name:1|c")
      }
    case Cancel ⇒ (context stop self)
  }
}

class BatchProducer extends ActorPublisher[Vector[Item]] with ActorLogging {

  import BatchProducer._

  import scala.concurrent.duration._

  val rnd = ThreadLocalRandom.current()

  override def receive = run(0l)

  private def run(id: Long): Receive = {
    case Request(n) ⇒ (context become requesting(id))
    case Cancel     ⇒ context.stop(self)
  }

  def requesting(id: Long): Receive = {
    log.info("requesting {}", id)
    //Making external call starting from id
    /*Http(context.system).singleRequest(HttpRequest(...)).flatMap { response =>
      Unmarshal(response.entity).to[Result]
    }.pipeTo(self)*/

    context.system.scheduler.scheduleOnce(100 millis) {
      var i = 0
      val batch = Vector.fill(rnd.nextInt(1, 10)) {
        i += 1; Item(i)
      }
      self ! Result(id + 1, batch.size, batch)
    }(context.system.dispatchers.lookup("akka.flow-dispatcher"))

    {
      case Result(lastId, size, items) ⇒
        //println(s"Produce:$lastId - $size")
        onNext(items)

        if (lastId == 0) onCompleteThenStop() //No items left.
        else if (totalDemand > 0) (context become requesting(lastId))
        else (context become run(lastId))

      case Cancel ⇒ context.stop(self)
    }
  }
}

object BatchProducer {

  case class Result(lastId: Long, size: Int, items: Vector[Item])

  case class Item(num: Int)

  def props: Props = Props[BatchProducer].withDispatcher("akka.flow-dispatcher")
}