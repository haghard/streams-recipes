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
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import recipes.BatchProducer.Item
import recipes.BalancerRouter.DBObject

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.language.postfixOps
import scala.util.{ Failure, Success }
import scalaz.{ \/-, -\/, \/ }

//runMain recipes.AkkaRecipes
object AkkaRecipes extends App {

  //https://groups.google.com/forum/#!topic/akka-user/MdvVehdco94

  val config = ConfigFactory.parseString(
    """
      |akka {
      |  stream.materializer.auto-fusing=off
      |
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

  val config20 = ConfigFactory.parseString(
    """
      |akka {
      |
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

  val statsD = new InetSocketAddress(InetAddress.getByName("192.168.0.47"), 8125)

  def sys: ActorSystem = ActorSystem("Sys", ConfigFactory.empty().withFallback(config))
  def sys20: ActorSystem = ActorSystem("Sys20", ConfigFactory.empty().withFallback(config20))

  implicit val system = sys20

  val decider: akka.stream.Supervision.Decider = {
    case ex: Throwable ⇒
      println(ex.getMessage)
      akka.stream.Supervision.Stop
  }

  val Settings = ActorMaterializerSettings.create(system = sys)
    .withInputBuffer(32, 32)
    .withSupervisionStrategy(decider)
    .withDispatcher("akka.flow-dispatcher")

  val Settings20 = ActorMaterializerSettings.create(system = sys)
    .withInputBuffer(32, 32)
    .withSupervisionStrategy(decider)
    .withDispatcher("akka.flow-dispatcher")

  val Mat = ActorMaterializer(Settings)
  //implicit val Mat20 = ActorMaterializer(Settings20)

  RunnableGraph.fromGraph(scenario0).run()(Mat)

  //RunnableGraph.fromGraph(scenario1).run()(Mat)
  //RunnableGraph.fromGraph(scenario2).run()
  //RunnableGraph.fromGraph(scenario3).run()
  //RunnableGraph.fromGraph(scenario4).run()(Mat20)

  //RunnableGraph.fromGraph(scenario7).run()
  //RunnableGraph.fromGraph(scenario15).run()

  /**
   * Tumbling windows discretize a stream into non-overlapping windows
   * Using conflate as rate detached operation
   */
  def tumblingWindow[T](name: String, duration: FiniteDuration): Sink[T, Unit] =
    (Flow[T].conflate(_ ⇒ 0l)((counter, _) ⇒ counter + 1l)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left))
      .to(Sink.foreach(acc ⇒ println(s"$name number:$acc")))
      .withAttributes(Attributes.inputBuffer(1, 1))

  def tumblingWindowWithFilter[T](name: String, duration: FiniteDuration)(filter: Long ⇒ Boolean): Sink[T, Unit] =
    (Flow[T].conflate(_ ⇒ 0l)((counter, _) ⇒ counter + 1l)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left))
      .to(Sink.foreach { acc ⇒ if (filter(acc)) println(s"$name number:$acc satisfied") else println(s"number:$acc unsatisfied") })
      .withAttributes(Attributes.inputBuffer(1, 1))

  /**
   * Sliding windows discretize a stream into overlapping windows
   * Using conflate as rate detached operation
   */
  def slidingWindow[T](name: String, duration: FiniteDuration, numOfTimeUnits: Long = 5): Sink[T, Unit] = {
    val nano = 1000000000
    (Flow[T].conflate(_ ⇒ 0l)((counter, _) ⇒ counter + 1l)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left))
      .scan((0l, 0, System.nanoTime())) { case ((acc, iter, last), v) ⇒ if (iter == numOfTimeUnits - 1) (v, 0, System.nanoTime()) else (acc + v, iter + 1, last) }
      .to(Sink.foreach { case (acc, iter, ts) ⇒ println(buildProgress(iter, acc, (System.nanoTime() - ts) / nano)) })
      .withAttributes(Attributes.inputBuffer(1, 1))
  }

  /**
   *
   */
  def allWindow[T](name: String, duration: FiniteDuration): Sink[T, Unit] =
    (Flow[T].conflate(_ ⇒ 0l)((counter, _) ⇒ counter + 1l)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left))
      .scan(0l)(_ + _)
      .to(Sink.foreach(acc ⇒ println(s"$name: $acc")))
      .withAttributes(Attributes.inputBuffer(1, 1))

  /**
    *
    *
    */
  def zipLast[T](in1: Source[T, Any], in2: Source[T, Any], in3: Source[T, Any]): Source[(T, T, T), Unit] = {
    Source.fromGraph(GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      //send duplicates in downstream
      def expand = b.add(Flow[T].expand[T, T](identity)(t ⇒ (t, t)))

      def conflate = b.add(Flow[T].conflate(identity)((c, _) ⇒ c))

      val zip = b.add(ZipWith(Tuple3.apply[T, T, T] _).withAttributes(Attributes.inputBuffer(1, 1)))

      in1 ~> conflate ~> zip.in0
      in2 ~> conflate ~> zip.in1
      in3 ~> conflate ~> zip.in2

      SourceShape(zip.out)
    })
  }

  def scenario0: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      //Source.unfoldInf(0)((d) ⇒ (d + 1, d))
      val fastIn = Source.tick(1.second, 1.second, ()).scan(0)((d, _) ⇒ d + 1)
      val slowIn = Source.tick(2.second, 2.second, ()).scan(0)((d, _) ⇒ d + 1)
      val evenSlowerIn = Source.tick(3.second, 3.second, ()).scan(0)((d, _) ⇒ d + 1)

      zipLast(fastIn, slowIn, evenSlowerIn) ~> Sink.actorSubscriber(SyncActor.props2("akka-sink-0", statsD))
      ClosedShape
    }
  }

  private def buildProgress(i: Int, acc: Long, sec: Long) =
    s"${List.fill(i)(" ★ ").mkString} number:$acc interval:$sec"

  /**
   * Situation: A source and a sink perform on the same rates.
   * Result: The source and the sink are going on the same rate.
   */
  def scenario1: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 20 milliseconds, Int.MaxValue, "akka-source1")
      val sink = Sink.actorSubscriber(SyncActor.props2("akka-sink1", statsD))

      /*slidingWindow("akka-scenario1", 2 seconds)*/
      (source alsoTo tumblingWindowWithFilter("akka-scenario1", 2 seconds) { _ >= 100l }) ~> sink
      ClosedShape
    }
  }

  /**
   * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
   * We are using buffer with OverflowStrategy.backpressure between them, which provide backpressure.
   * Result: The source's rate is going to decrease proportionally with the sink's rate.
   *
   */
  def scenario2: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source2")
      val degradingSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink2", statsD, 1l))
      val buffer = Flow[Int].buffer(1 << 7, OverflowStrategy.backpressure)
      (source alsoTo allWindow("akka-scenario2", 5 seconds)) ~> buffer ~> degradingSink
      ClosedShape
    }
  }

  /**
   * Situation: A source and a sink perform on the same rate in the beginning, the sink gets slower later, increases delay with every message.
   * We are using buffer with OverflowStrategy.dropHead  between them, it will drop the oldest items.
   * Result: The sink's rate is going to be decrease but the source's rate will be stayed on the initial level.
   */
  def scenario3: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source3")
      val slowingSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink3", statsD, 1l))
      //OverflowStrategy.dropHead will drop the oldest waiting job
      //OverflowStrategy.dropTail will drop the youngest waiting job
      val buffer = Flow[Int].buffer(1 << 7, OverflowStrategy.dropHead)
      (source alsoTo allWindow("akka-scenario3", 5 seconds)) ~> buffer ~> slowingSink
      ClosedShape
    }
  }

  def scenario4: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, Int.MaxValue, "akka-source4")
      val fastSink = Sink.actorSubscriber(SyncActor.props("akka-sink4_fast", statsD, 0l))
      val slowSink = Sink.actorSubscriber(DegradingActor.props2("akka-sink4_slow", statsD, 1l))

      //I want branches to be run in parallel
      val broadcast = builder.add(Broadcast[Int](2).addAttributes(Attributes.asyncBoundary))

      (source alsoTo allWindow("akka-scenario4", 5 seconds)) ~> broadcast ~> fastSink
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
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 10 milliseconds, 20000, "akka-source5")

      val fastSink = Sink.actorSubscriber(SyncActor.props("akka-sink5_0", statsD, 0l))
      val degradingSink1 = Sink.actorSubscriber(DegradingActor.props2("akka-sink5_1", statsD, 8l))
      val degradingSink2 = Sink.actorSubscriber(DegradingActor.props2("akka-sink5_2", statsD, 10l))

      //val buffer = Flow[Int].buffer(1000, OverflowStrategy.dropTail)
      //val buffer = Flow[Int].buffer(128, OverflowStrategy.backpressure)
      /*
      source.to(
        Sink.combine(
          fastSink,
          Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) to degradingSink1,
          Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) to degradingSink2
        )(Broadcast[Int](_))
      )*/

      val broadcast = builder.add(Broadcast[Int](3))

      // connect source to sink with additional step
      source ~> broadcast ~> fastSink
      broadcast ~> Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) ~> degradingSink1
      broadcast ~> Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) ~> degradingSink2
      ClosedShape
    }
  }

  def scenario6: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
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
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val merger = b.add(Merge[Int](sources.size))
        sources.zipWithIndex.foreach {
          case (src, idx) ⇒ b.add(src) ~> merger.in(idx)
        }
        SourceShape(merger.out)
      }
    )

    val queryStreams = Source.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val merge = b.add(Merge[Int](srcs.size))
        srcs.foreach { name ⇒
          Source.actorPublisher(Props(classOf[TopicReader], name, statsD, latencies.next())
            .withDispatcher("akka.flow-dispatcher")) ~> merge
        }
        SourceShape(merge.out)
      }
    )

    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
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
      (queryStreams alsoTo allWindow("akka-scenario7", 1 seconds)) ~> Sink.actorSubscriber(SyncActor.props("akka-sink7", statsD, 0l))
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      //val balancer = b.add(new RoundRobinStage4[Int].withAttributes(buffAttributes))
      val balancer = b.add(Balance[Int](parallelism))
      val merge = b.add(Merge[Int](parallelism).withAttributes(buffAttributes))

      (source alsoTo allWindow("akka-scenario8", 5 seconds)) ~> balancer

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
    val source = throttledSrc(statsD, 1 second, 100 milliseconds, Int.MaxValue, "akka-source-08")
    val errorSink = Sink.actorSubscriber(SyncActor.props("akka-sink-error08", statsD, 1l)) //slow sink
    val sink = Sink.actorSubscriber(SyncActor.props("akka-sink-08", statsD, 0l))

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      (aggregatedSource via throttledFlow(500 milliseconds)) ~> sink
      ClosedShape
    }
  }

  case class State(totalSamples: Long, sum: Long) {
    def combine(current: Long) = this.copy(this.totalSamples + 1, this.sum + current)
  }

  def every[T](interval: FiniteDuration): Flow[T, T, Unit] =
    Flow.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val zip = b.add(ZipWith[T, Unit, T](Keep.left).withAttributes(Attributes.inputBuffer(1, 1)))
        val dropOne = b.add(Flow[T].drop(1))
        Source.tick(Duration.Zero, interval, ()) ~> zip.in1
        zip.out ~> dropOne.in
        FlowShape(zip.in0, dropOne.outlet)
      }
    )

  /**
   * Almost same as ``every``
   */
  def throttledFlow[T](interval: FiniteDuration): Flow[T, T, Unit] = {
    Flow.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
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
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val source = throttledSrc(statsD, 1 second, 20 milliseconds, Int.MaxValue, "akka-source10")
      val sink = Sink.actorSubscriber(DegradingActor.props2("akka-sink10", statsD, 0l))
      source ~> heartbeats(50.millis, 0) ~> sink
      ClosedShape
    }

  def heartbeats[T](interval: FiniteDuration, zero: T): Flow[T, T, Unit] =
    Flow.fromGraph(
      GraphDSL.create() { implicit builder ⇒
        import GraphDSL.Implicits._
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
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
  def scenario13(mat: ActorMaterializer): Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      //no backpressure there, just dropping
      val (actor, publisher) = Source.actorRef[Int](200, OverflowStrategy.dropTail)
        .toMat(Sink.asPublisher[Int](false))(Keep.both).run()(mat)

      val i = new AtomicInteger()
      sys.scheduler.schedule(1 second, 20 milliseconds) {
        i.getAndIncrement()
        if (i.get() < Int.MaxValue) actor ! i
      }(mat.executionContext)

      Source.fromPublisher(publisher) ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink13", statsD, 13l))
      ClosedShape
    }
  }

  /**
   * External Producer through Source.queue
   */
  def scenario13_1(mat: ActorMaterializer): Graph[ClosedShape, Unit] = {
    implicit val Ctx = mat.executionContext
    implicit val ExtCtx = sys.dispatchers.lookup("akka.blocking-dispatcher")

    val pubStatsD = new StatsD {
      override val address = statsD
    }
    val (queue, publisher) = Source.queue[Option[Int]](1 << 7, OverflowStrategy.backpressure)
      .takeWhile(_.isDefined).map(_.get)
      .toMat(Sink.asPublisher[Int](false))(Keep.both).run()(mat)

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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      Source.fromPublisher(publisher) ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink13", statsD, 1l))
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

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
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
   *                                                  Parallel
   *                                                  +------+
   *                                               +--|Worker|--+
   *                                               |  +------+  |
   * +-----------------+     +--------------+      |  +------+  |  +-----------+
   * |DbCursorPublisher|-----|BalancerRouter|------|--|Worker|-----|RecordsSink|
   * +-----------------+     +--------------+      |  +------+  |  +-----------+
   *                                               |  +------+  |
   *                                               +--|Worker|--+
   *                                                  +------+
   */
  def scenario15: Graph[ClosedShape, Unit] = {
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val out = sys.actorOf(Props(classOf[RecordsSink], "sink15", statsD).withDispatcher("akka.flow-dispatcher"), "akka-sink15")
      val src = Source.actorPublisher[Long](Props(classOf[DbCursorPublisher], "akka-source15", 20000l, statsD).withDispatcher("akka.flow-dispatcher"))
        .map(DBObject(_, out))
      src ~> Sink.actorSubscriber(BalancerRouter.props)
      ClosedShape
    }
  }

  /**
   * 2 subscribers for single file from tail
   */
  def scenario16(mat: ActorMaterializer): Graph[ClosedShape, Unit] = {
    val log = "./example.log"
    val n = 50l
    implicit val ec = mat.executionContext

    def tailer: Source[String, Unit] = {
      val proc = new java.lang.ProcessBuilder()
        .command("tail", s"-n$n", "-f", log)
        .start()
      proc.getOutputStream.close()
      val input = proc.getInputStream

      def readChunk(): scala.concurrent.Future[ByteString] = Future {
        val buffer = new Array[Byte](1024 * 6)
        val read = (input read buffer)
        println(s"available: $read")
        if (read > 0) ByteString.fromArray(buffer, 0, read) else ByteString.empty
      }

      val publisher = Source.repeat(0)
        .mapAsync(1)(_ ⇒ readChunk())
        .takeWhile(_.nonEmpty)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 10000, allowTruncation = true))
        .transform(() ⇒ new PushStage[ByteString, ByteString] {
          override def onPush(elem: ByteString, ctx: Context[ByteString]): SyncDirective = ctx.push(elem ++ ByteString('\n'))
          override def postStop(): Unit = {
            println("tail has done")
            proc.destroy()
          }
        })
        .runWith(Sink.asPublisher(true))(mat)

      Source.fromPublisher(publisher).map { x ⇒ x.utf8String.slice(6, 80) + "..." }
    }

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[String](2))
      tailer ~> broadcast ~> Sink.actorSubscriber[String](SyncActor.props4("akka-source16_0", statsD, 500l, n))
      broadcast ~> Flow[String].buffer(32, OverflowStrategy.backpressure) ~> Sink.actorSubscriber[String](SyncActor.props4("akka-source16_1", statsD, 1000l, n))
      ClosedShape
    }
  }

  /**
   * Create a source which is throttled to a number of message per second.
   */
  def throttledSrc(statsD: InetSocketAddress, delay: FiniteDuration, interval: FiniteDuration, limit: Int, name: String): Source[Int, Unit] = {
    Source.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val sendBuffer = ByteBuffer.allocate(1024)
        val channel = DatagramChannel.open()

        // two sources
        val tickSource = Source.tick(delay, interval, ())
        val rangeSource = Source.fromIterator(() ⇒ Iterator.range(1, limit))

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

object BalancerRouter {

  case class DBObject(id: Long, replyTo: ActorRef)

  case class Work(id: Long)

  case class Reply(id: Long)

  case class Done(id: Long)

  def props: Props = Props(new BalancerRouter).withDispatcher("akka.flow-dispatcher")
}

/**
 * Manually managed router
 */
class BalancerRouter extends ActorSubscriber with ActorLogging {
  import BalancerRouter._

  val MaxInFlight = 32
  var requestors = Map.empty[Long, ActorRef]
  val n = Runtime.getRuntime.availableProcessors() / 2

  val workers = (0 until n)
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
  import BalancerRouter._

  override def receive = {
    case Work(id) ⇒
      Thread.sleep(ThreadLocalRandom.current().nextInt(100, 150))
      //log.info("{} has done job {}", name, id)
      sender() ! Reply(id)
  }
}

class RecordsSink(name: String, val address: InetSocketAddress) extends Actor with ActorLogging with StatsD {
  override def receive = {
    case BalancerRouter.Done(id) ⇒
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

  def props4(name: String, address: InetSocketAddress, delay: Long, limit: Long) =
    Props(new SyncActor(name, address, delay, limit)).withDispatcher("akka.flow-dispatcher")
}

class SyncActor private (name: String, val address: InetSocketAddress, delay: Long, limit: Long) extends ActorSubscriber with StatsD {
  var count = 0
  override protected val requestStrategy = OneByOneRequestStrategy

  private def this(name: String, statsD: InetSocketAddress, delay: Long) {
    this(name, statsD, delay, 0l)
  }

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0l)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      send(s"$name:1|c")

    case OnNext(msg: (Int, Int, Int)) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: String) ⇒
      println(msg)
      Thread.sleep(delay)
      count += 1
      if (count == limit) {
        println(s"Limit $limit has been achieved")
        context.system.stop(self)
      }

    case OnError(ex) ⇒
      println(s"OnError SyncActor: ${ex.getMessage}")
      context.system.stop(self)

    case OnComplete ⇒
      println(s"Complete SyncActor")
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
      log.info("request: {}", n)
      if (seqN >= end)
        onCompleteThenStop()

      limit = n
      while (limit > 0) {
        seqN += 1
        limit -= 1

        if (limit % showPeriod == 0) {
          log.info("Cursor progress: {}", seqN)
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