package recipes

import java.io.{File, FileInputStream}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor._
import akka.actor.typed.{Behavior, PreRestart}
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.stream._
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.stream.typed.scaladsl.{ActorSink, ActorSource}
import akka.util.ByteString
import com.esri.core.geometry.Point
import com.typesafe.config.ConfigFactory
import org.reactivestreams.Publisher
import recipes.AkkaRecipes.{CircularFifo, LogEntry, SimpleMAState}
import recipes.BalancerRouter._
import recipes.BatchProducer.Item
import recipes.ConsistentHashingRouter.{CHWork, DBObject2}
import recipes.CustomStages.{BackPressuredStage, BackpressureMeasurementStage, DisjunctionStage, DropHeadStage, DropTailStage, QueueSrc, SimpleRingBuffer}
import recipes.DegradingTypedActorSink.{IntValue, Protocol, RecoverableSinkFailure}
import recipes.Sinks.{DegradingGraphiteSink, GraphiteSink, GraphiteSink3}

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.{Deadline, FiniteDuration, _}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.control.NoStackTrace
import scala.util.{Failure, Success, Try}

//runMain recipes.AkkaRecipes
object AkkaRecipes extends App {

  val FixedDispatcher  = "akka.fixed-dispatcher"
  val PinnedDispatcher = "akka.pinned-dispatcher"

  val config = ConfigFactory.parseString(
    """
      |akka {
      |
      |  flow-dispatcher {
      |    type = Dispatcher
      |    executor = "fork-join-executor"
      |    fork-join-executor {
      |      parallelism-min = 2
      |      parallelism-max = 4
      |    }
      |  }
      |
      |  fixed-dispatcher {
      |    executor = "thread-pool-executor"
      |    thread-pool-executor {
      |     fixed-pool-size = 3
      |    }
      |    throughput = 50
      |  }
      |
      |  resizable-dispatcher {
      |     type = Dispatcher
      |     executor = "thread-pool-executor"
      |     thread-pool-executor {
      |       core-pool-size-min = 5
      |       core-pool-size-factor = 2.0
      |       core-pool-size-max = 10
      |     }
      |     throughput = 100
      |  }
      |
      | pinned-dispatcher {
      |   type = PinnedDispatcher
      |   executor = "thread-pool-executor"
      | }
      |
      |}
    """.stripMargin
  )

  val ms =
    new InetSocketAddress(InetAddress.getByName("127.0.0.1" /*"192.168.77.83"*/ ), 8125)

  def sys: ActorSystem =
    ActorSystem("streams", ConfigFactory.empty().withFallback(config).withFallback(ConfigFactory.load()))

  val decider: akka.stream.Supervision.Decider = {
    case ex: Throwable ⇒
      ex.printStackTrace()
      println(s"Streaming error: ${ex.getMessage}")
      akka.stream.Supervision.Stop
  }

  val Settings = ActorMaterializerSettings
    .create(system = sys)
    //.withInputBuffer(32, 32)
    .withInputBuffer(16, 16)
    .withSupervisionStrategy(decider)
    .withDispatcher("akka.flow-dispatcher")

  //https://gist.github.com/debasishg/8172796
  type CircularFifo[T] = org.apache.commons.collections4.queue.CircularFifoQueue[T]

  /*RunnableGraph.fromGraph(scenario21())
    //.withAttributes(ActorAttributes.supervisionStrategy(_ => akka.stream.Supervision.Resume)) // The stream must not die!
    .run()(ActorMaterializer(Settings)(sys))*/

  //RunnableGraph.fromGraph(scenario22(sys)).run()(ActorMaterializer(Settings)(sys))

  //RunnableGraph.fromGraph(scenario23(sys)).run()(ActorMaterializer(Settings)(sys))

  val mat: Materializer = ActorMaterializer(Settings)(sys)
  implicit val ec       = mat.executionContext

  //scenario31

  //scenario7_1(mat)
  //scenario7_2(mat)
  //scenario7_3(mat)

  //typeAsk.runWith(Sink.ignore)(mat).onComplete(_ ⇒ println("done"))

  //typedActorSrc(mat)

  //RunnableGraph.fromGraph(scenario24).run()(ActorMaterializer(Settings)(sys))
  //RunnableGraph.fromGraph(scenario25).run()(ActorMaterializer(Settings)(sys))
  //RunnableGraph.fromGraph(scenario26).run()(ActorMaterializer(Settings)(sys))

  //scenario2_1
  RunnableGraph.fromGraph(scenario31).run()(ActorMaterializer(Settings)(sys))

  case object CurrentThreadExecutionContext extends ExecutionContextExecutor {
    def execute(runnable: Runnable): Unit     = runnable.run()
    def reportFailure(cause: Throwable): Unit = throw cause
  }
  /*
  val graph = scenario27().run()(ActorMaterializer(Settings)(sys))
  val enqueue = (elem: Int) ⇒ {
    //println(s"${Thread.currentThread.getName}: $elem")
    /*{
      implicit val ec = akka.dispatch.ExecutionContexts.sameThreadExecutionContext // CurrentThreadExecutionContext
      elem.handler.map { x => SignalAndHandler(elem.signal, Success(x)) }
        .recover { case x => SignalAndHandler(elem.signal, Failure(x)) }
    }*/

    val future: Future[Handler[Int]] =
      /*Future {
        Thread.sleep(100)
        println(s"${Thread.currentThread.getName}: remote call0:$elem")
        elem
      }(CurrentThreadExecutionContext)*/
      Future
        .successful(elem)
        .map { e ⇒
          println(s"${Thread.currentThread.getName}: remote call: $e")
          Thread.sleep(200)
          Handler(Success(e))
        }(CurrentThreadExecutionContext)

    graph.offer(future).onComplete {
      case Success(r /*QueueOfferResult.Enqueued*/ ) ⇒
        println(s"${Thread.currentThread.getName}: $r: $elem")
      //case Success(QueueOfferResult.Dropped) ⇒ println("Dropped")
      case Failure(error) ⇒
        println(s"Error: $error")
    }
    //Thread.sleep(200)
  }

  (0 to 50).foreach(enqueue)

  Thread.sleep(5000)
  graph.complete
  graph.watchCompletion().onComplete { _ ⇒
    println("Completion !!!!")
  }*/

  /**
    * Tumbling windows discretize a stream into non-overlapping windows
    * Using conflate as rate detached operation
    */
  def tumblingWindow[T](name: String, duration: FiniteDuration): Sink[T, akka.NotUsed] =
    Flow[T]
      .conflateWithSeed(_ ⇒ 0L)((counter, _) ⇒ counter + 1L)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left)
      .to(Sink.foreach(acc ⇒ println(s"$name count:$acc")))
      .withAttributes(Attributes.inputBuffer(1, 1))

  def tumblingWindowWithFilter[T](name: String, duration: FiniteDuration)(
    filter: Long ⇒ Boolean
  ): Sink[T, akka.NotUsed] =
    Flow[T]
      .conflateWithSeed(_ ⇒ 0L)((counter, _) ⇒ counter + 1L)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left)
      .to(Sink.foreach { acc ⇒
        if (filter(acc)) println(s"$name count:$acc satisfied")
        else println(s"number:$acc unsatisfied")
      })
      .withAttributes(Attributes.inputBuffer(1, 1))

  /**
    * Sliding windows discretize a stream into overlapping windows
    * Using conflate as rate detached operation
    */
  def slidingWindow[T](name: String, duration: FiniteDuration, numOfIntervals: Long = 5): Sink[T, akka.NotUsed] = {
    val nano = 1000000000
    Flow[T]
      .conflateWithSeed(_ ⇒ 0L)((counter, _) ⇒ counter + 1L)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left)
      .scan((0L, 0, System.nanoTime)) {
        case ((acc, iter, lastTs), v) ⇒
          if (iter == numOfIntervals - 1) (v, 0, System.nanoTime)
          else (acc + v, iter + 1, lastTs)
      }
      .to(Sink.foreach { case (acc, iter, ts) ⇒ println(buildProgress(iter, acc, (System.nanoTime - ts) / nano)) })
      .withAttributes(Attributes.inputBuffer(1, 1))
  }

  /**
    *
    */
  def countElementsWindow[T](name: String, duration: FiniteDuration): Sink[T, akka.NotUsed] =
    Flow[T]
      .conflateWithSeed(_ ⇒ 0L)((counter, _) ⇒ counter + 1L)
      .zipWith(Source.tick(duration, duration, ()))(Keep.left)
      .scan(0L)(_ + _)
      .to(Sink.foreach(acc ⇒ println(s"$name: $acc")))
      .withAttributes(Attributes.inputBuffer(1, 1))

  private def buildProgress(i: Int, acc: Long, sec: Long) =
    s"${List.fill(i)(" ★ ").mkString} number of elements:$acc time window:$sec sec"

  /**
    * Situation:
    * We have 3 sources with different rates.
    * We use conflate stage before zip, hence we constantly update the last element for every source in the tuple.
    * When zip stage is getting onNext signal it sends the tuple with latest values inside.
    *
    * Result:
    * The source's rates stay the same as it was at the beginning. Sink performs on rate that equals to the slowest source.
    */
  def scenario0: Graph[ClosedShape, akka.NotUsed] = {

    def last3[T](
      in1: Source[T, akka.NotUsed],
      in2: Source[T, akka.NotUsed],
      in3: Source[T, akka.NotUsed]
    ): Source[(T, T, T), akka.NotUsed] =
      Source.fromGraph(
        GraphDSL.create() { implicit b ⇒
          import GraphDSL.Implicits._

          def conflate: FlowShape[T, T] =
            b.add(
              Flow[T]
                .withAttributes(Attributes.inputBuffer(1, 1))
                .conflateWithSeed(identity)((c, _) ⇒ c)
            )

          val zip = b.add(ZipWith(Tuple3.apply[T, T, T] _).withAttributes(Attributes.inputBuffer(1, 1)))

          in1 ~> conflate ~> zip.in0
          in2 ~> conflate ~> zip.in1
          in3 ~> conflate ~> zip.in2

          SourceShape(zip.out)
        }
      )

    val fastest = timedSource(ms, 0 second, 10.milliseconds, Int.MaxValue, "source_0_fst")
    val middle  = timedSource(ms, 0 second, 15.milliseconds, Int.MaxValue, "source_0_mid")
    val slowest = timedSource(ms, 0 second, 20.milliseconds, Int.MaxValue, "source_0_slow")

    last3(fastest, middle, slowest)
      .alsoTo(countElementsWindow[(Int, Int, Int)]("akka-scenario0", 2 seconds))
      .async
      .to(new GraphiteSink3("sink_0", 0, ms))
  }

  /**
    * A source and a sink run at the same rate.
    */
  def scenario1: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      val source = timedSource(ms, 1 second, 20 milliseconds, Int.MaxValue, "source_1")
      val sink   = new GraphiteSink[Int]("sink_1", 0, ms)

      (source alsoTo tumblingWindowWithFilter("akka-scenario1", 2 seconds)(_ >= 97L)) ~> sink
      ClosedShape
    }

  /**
    * Situation:
    * A source and a sink run at the same rate at the beginning, later the sink gets slower increasing the delay with every message.
    * We are using buffer with {{{OverflowStrategy.backpressure}}} between them.
    * Result: The source rate is going to decrease proportionately to the sink rate.
    */
  def scenario2: Graph[ClosedShape, akka.NotUsed] = {
    val src           = timedSource(ms, 0 second, 10 milliseconds, Int.MaxValue, "source_2")
    val degradingSink = new DegradingGraphiteSink[Int]("sink_2", 1L, ms)
    val buffer        = Flow[Int].buffer(1 << 7, OverflowStrategy.backpressure).async

    src
      .alsoTo(countElementsWindow("akka-scenario2", 5 seconds))
      .via(buffer)
      .to(degradingSink)

    /*GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._
      val source = timedSource(ms, 0 second, 10 milliseconds, Int.MaxValue, "source_2")
      val degradingSink = new DegradingGraphiteSink("sink_2", 1l, ms)
      val buffer = Flow[Int].buffer(1 << 7, OverflowStrategy.backpressure).async

      (source alsoTo allWindow("akka-scenario2", 5 seconds)) ~> buffer ~> degradingSink
      ClosedShape
    }*/
  }

  //same as scenario2 but with InternalBufferStage
  def scenario2_1: Graph[ClosedShape, akka.NotUsed] = {
    val source = timedSource(ms, 10.milliseconds, 10.milliseconds, Int.MaxValue, "source_2_1")
    val sink   = new DegradingGraphiteSink[Int]("sink_2_1", 1L, ms)

    source
      .alsoTo(countElementsWindow("akka-scenario2_1", 10 seconds))
      .via(new BackPressuredStage[Int](1 << 7))
      .async
      .to(sink)
  }

  /**
    * Situation: The source and sink run at the same rate at the beginning, the sink gets slower increasing the delay with every message.
    * We are using buffer with OverflowStrategy.dropHead.
    * It will drop the oldest items.
    * Result: The sink rate is going to be decreased but the source rate stays at the initial level.
    */
  def scenario3: Graph[ClosedShape, akka.NotUsed] = {
    val source        = timedSource(ms, 10 milliseconds, 10 milliseconds, Int.MaxValue, "source_3")
    val degradingSink = new DegradingGraphiteSink[Int]("sink_3", 2L, ms)
    val buffer        = Flow[Int].buffer(1 << 7, OverflowStrategy.dropHead)

    source
      .alsoTo(countElementsWindow("akka-scenario3", 5 seconds))
      .via(buffer)
      .async
      .to(degradingSink)

    /*GraphDSL.create() { implicit builder ⇒
      import GraphDSL.Implicits._
      //OverflowStrategy.dropHead will drop the oldest waiting job
      //OverflowStrategy.dropTail will drop the youngest waiting job
      (source alsoTo allWindow("akka-scenario3", 5 seconds)) ~> buffer ~> degradingSink
      ClosedShape
    }*/
  }

  /**
    * Fast source and 2 sinks. The first sink is fast whereas the second is degrading.
    * Result: The pipeline's rate is going to be decreased up to the slow one.
    */
  def scenario4: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val source   = timedSource(ms, 1 second, 10 milliseconds, Int.MaxValue, "source_4")
      val fastSink = new GraphiteSink[Int]("sink_4", 0, ms)
      val slowSink = new DegradingGraphiteSink[Int]("sink_4_deg", 1L, ms)

      val bcast = b.add(akka.stream.scaladsl.Broadcast[Int](2)) //.addAttributes(Attributes.asyncBoundary)

      (source alsoTo countElementsWindow("akka-scenario4", 5 seconds)) ~> bcast ~> fastSink
      bcast ~> slowSink
      ClosedShape
    }

  /**
    *
    *
    * We want the first sink to be the primary sink (source of true), whereas 2th and 3th sinks to be the best effort (some elements)
    *
    * Fast publisher and 3 sinks. The first sink runs as fast as it can, whereas 2th and 3th degrade over time.
    * 2th and 3th sinks get messages through buffers with OverflowStrategy.dropTail strategy, therefore the
    * overall pipeline rate doesn't degrade.
    *
    */
  def scenario5: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val source = timedSource(ms, 0 second, 10 milliseconds, Int.MaxValue, "source_5")

      val fastSink       = new GraphiteSink[Int]("sink_5", 0L, ms)
      val degradingSink1 = new DegradingGraphiteSink[Int]("sink_5_1_deg_0", 1L, ms)
      val degradingSink2 = new DegradingGraphiteSink[Int]("sink_5_1_deg_1", 2L, ms)

      /*
      source.to(
        Sink.combine(
          fastSink,
          Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) to degradingSink1,
          Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail) to degradingSink2
        )(Broadcast[Int](_))
      )*/

      val bcast  = b.add(Broadcast[Int](3) /*.addAttributes(Attributes.asyncBoundary)*/ )
      val buffer = b.add(Flow[Int].buffer(1 << 7, OverflowStrategy.dropTail))

      source ~> bcast ~> fastSink
      bcast ~> buffer ~> degradingSink1
      bcast ~> buffer ~> degradingSink2
      ClosedShape
    }

  /**
    * Fast source is connected with two sinks through balance stage.
    * We use Balance to achieve parallelism here.
    * The first sink is fast whereas the second is degrading.
    */
  def scenario6: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val source = timedSource(ms, 0 milliseconds, 10 milliseconds, Int.MaxValue, "source_6")

      val fastSink = new GraphiteSink[Int]("sink_6_0", 0L, ms)
      val slowSink = new DegradingGraphiteSink[Int]("sink_6_1", 2L, ms)

      val balancer = b.add(Balance[Int](2))

      source ~> balancer ~> fastSink
      balancer ~> slowSink
      ClosedShape
    }

  /**
    *
    * Merge[In] – (N inputs, 1 output) picks randomly from inputs pushing them one by one to its output
    * Several sources with different rates fan-in in single merge followed by sink
    * Result: Sink rate = sum(sources)
    *
    *
    * src1 merge src2 - merge 2 src
    * src1 zipWith src2 zipFunc  - merge 2 src + some work
    *
    * Use MergeHub.source[Int] for dynamic number of sources
    *
    * val hub = MergeHub.source[Int]
    * .to(Sink.actorSubscriber[Int](SyncActor.props("akka-sink7", ms, 0l)))
    * .run()
    *
    * def connect(src: Source[Int, NotUsed]) =
    *       src.to(hub).run()
    *
    */
  def scenario7: Graph[ClosedShape, akka.NotUsed] = {
    val latencies = List(20L, 30L, 40L, 45L).iterator
    val names     = List("akka-source7_0", "akka-source7_1", "akka-source7_2", "akka-source7_3")

    lazy val sources = names.map { name ⇒
      Source.actorPublisher[Int](
        Props(classOf[TopicReader], name, ms, latencies.next())
          .withDispatcher("akka.flow-dispatcher")
      )
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
        val merge = b.add(Merge[Int](names.size))
        names.foreach { name ⇒
          Source.actorPublisher(
            Props(classOf[TopicReader], name, ms, latencies.next())
              .withDispatcher("akka.flow-dispatcher")
          ) ~> merge
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
      (queryStreams alsoTo countElementsWindow("akka-scenario7", 1 seconds)) ~> Sink
        .actorSubscriber(SyncActor.props("akka-sink7", ms, 0L))
      ClosedShape
    }
  }

  /*
    Many to one with a dynamically growing number of sources
    MergeHub creates a source that emits elements merged from a dynamic set of producers
   */
  def scenario7_1(mat: Materializer): Unit = {
    implicit val ec       = mat.executionContext
    val numOfMsgPerSource = 300

    val manyToOneSink: Sink[Int, NotUsed] = Sink.actorRefWithAck[Int](
      sys.actorOf(DegradingActorSink.props("akka-sink-7_1", ms, 10)),
      onInitMessage = DegradingActorSink.Init,
      ackMessage = DegradingActorSink.Ack,
      onCompleteMessage = DegradingActorSink.OnCompleted,
      onFailureMessage = DegradingActorSink.StreamFailure(_)
    )

    def attachNSources(sink: Sink[Int, NotUsed], iterNum: Int, limit: Int = 5): Future[Unit] = {
      val f = akka.pattern.after(5.second, sys.scheduler)(
        Future {
          val src = timedSource(
            ms,
            0.millis,
            (iterNum * 100).millis,
            (iterNum * 10000) + numOfMsgPerSource,
            s"source_7_1-$iterNum",
            iterNum * 10000
          )
          //src.runWith(sink)(mat)
          src.to(sink).run()(mat)
          ()
        }
      )
      f.onComplete { _ ⇒
        if (iterNum < limit)
          attachNSources(sink, iterNum + 1, limit)
      }
      f
    }

    //StreamRefs

    //1. You have a source you'd like to
    /*val src = Source.fromIterator(() => Iterator.range(0, 10))
    src.runWith(StreamRefs.sourceRef())(mat).map { srcRef =>
      //here we should send srcRef to the remote side so that the remote side could do the following
      srcRef.source.to(Sink.foreach { el => }).run()(mat)
      //srcRef.source.runWith(Sink.foreach { el =>  })(mat)
      ()
    }*/

    //2.
    /*
    val sink = Sink.foreach[Int] { el: Int => }
    /*StreamRefs.sinkRef[Int]().to(sink).run().map { sinkRef =>
      val src = Source.fromIterator(() => Iterator.range(0, 10))
      src.to(sinkRef.sink).run()(mat)
    }*/

    sink.runWith(StreamRefs.sinkRef[Int]())(mat).map { sinkRef =>
      //
      val src = Source.fromIterator(() => Iterator.range(0, 10))
      src.to(sinkRef.sink).run()(mat)
    }*/

    //This sink can be materialized (ie. run) arbitrary times
    val sinkHub: Sink[Int, NotUsed] =
      MergeHub
        .source[Int](1 << 6)
        //.to(new GraphiteSink("sink_0", 0, ms))
        .to(manyToOneSink)
        .run()(mat)

    sinkHub.runWith(StreamRefs.sinkRef[Int]())(mat).map { sinkRef ⇒
      //send it to the remote side
      //replyTo ! sinkRef
      val src = Source.fromIterator(() ⇒ Iterator.range(0, 10))
      src.to(sinkRef.sink).run()(mat)
    }

    StreamRefs.sinkRef[Int]().to(sinkHub).run()(mat).map { sinkRef ⇒
      //on the remote side
      val src = Source.fromIterator(() ⇒ Iterator.range(0, 10))
      src.to(sinkRef.sink).run()(mat)
    }

    attachNSources(sinkHub, 1)
  }

  /*def aboveAverage: Flow[Double, Double, ] =
    Flow[Double].statefulMapConcat { () ⇒
      var sum = 0
      var n   = 0
      rating ⇒ {
        sum += rating
        n += 1
        val average = sum / n
        if (rating >= average) rating :: Nil
        else Nil
      }
    }*/

  case class Persisted(num: Long)
  case class PersistElement(num: Long, srcSender: akka.actor.typed.ActorRef[Persisted])

  def typeAsk: Source[Persisted, akka.NotUsed] = {
    import akka.actor.typed.scaladsl.adapter._
    import akka.actor.typed.scaladsl.Behaviors

    implicit val t = akka.util.Timeout(1.second)

    val dbRef: akka.actor.typed.ActorRef[PersistElement] =
      sys.spawn(
        Behaviors.receiveMessage[PersistElement] {
          case PersistElement(num, srcSender) ⇒
            println(num)
            Thread.sleep(500) //write operation
            //confirm the element
            srcSender.tell(Persisted(num))
            Behaviors.same
        },
        "actor-in-the-middle"
      )

    Source
      .repeat(42L)
      .via(
        akka.stream.typed.scaladsl.ActorFlow.ask[Long, PersistElement, Persisted](dbRef)(
          (elem: Long, srcSender: akka.actor.typed.ActorRef[Persisted]) ⇒ PersistElement(elem, srcSender)
        )
      )
      .take(100)
  }

  def typedActorSrc(implicit mat: Materializer) = {
    import akka.actor.typed.scaladsl.adapter._

    val ackTo: akka.actor.typed.ActorRef[DegradingTypedActorSource.Confirm] =
      sys.spawn(
        Behaviors.setup[DegradingTypedActorSource.Confirm] { ctx ⇒
          def awaitConfirmation(
            i: Int,
            src: akka.actor.typed.ActorRef[DegradingTypedActorSource.TypedSrcProtocol]
          ): Behavior[DegradingTypedActorSource.Confirm] =
            Behaviors.receiveMessage[DegradingTypedActorSource.Confirm] {
              case DegradingTypedActorSource.Confirm ⇒
                println("Confirm: " + i)
                Thread.sleep(1000)
                val next = i + 1
                src.tell(DegradingTypedActorSource.IntValue(next))
                awaitConfirmation(next, src)
            }

          Behaviors.receiveMessage[DegradingTypedActorSource.Confirm] {
            case DegradingTypedActorSource.Connect(src) ⇒
              src.tell(DegradingTypedActorSource.IntValue(0))
              awaitConfirmation(0, src)
          }

        },
        "src-actor"
      )

    //or
    //ActorSource.actorRef()

    //actor -> src -> flow -> sink

    val actorSrc: akka.actor.typed.ActorRef[DegradingTypedActorSource.TypedSrcProtocol] =
      ActorSource
        .actorRefWithAck[DegradingTypedActorSource.TypedSrcProtocol, DegradingTypedActorSource.Confirm](
          ackTo = ackTo,
          ackMessage = DegradingTypedActorSource.Confirm,
          completionMatcher = { case DegradingTypedActorSource.Completed      ⇒ CompletionStrategy.immediately },
          failureMatcher = { case DegradingTypedActorSource.StreamFailure(ex) ⇒ ex }
        )
        .to(Sink.foreach[DegradingTypedActorSource.TypedSrcProtocol](m ⇒ println("out: " + m)))
        .run()

    ackTo.tell(DegradingTypedActorSource.Connect(actorSrc))
  }

  //Dynamic Many to Many(fan-in fan-out)
  def scenario7_2(mat: Materializer): Unit = {
    implicit val ec       = mat.executionContext
    val numOfMsgPerSource = 3000

    //RestartFlow.withBackoff(1.second, 2.second, 0.3)

    //ActorSource without bp

    def typedActorBasedSink(n: Int): Sink[Int, NotUsed] = {
      import akka.actor.typed.scaladsl.adapter._
      import akka.actor.typed.scaladsl.Behaviors
      val name = s"akka-sink-7_2-$n"

      //val actorRef = DegradingTypedActorSink(name, GraphiteMetrics(ms), 10, 0)

      //https://doc.akka.io/docs/akka/current/typed/fault-tolerance.html#supervision
      //

      /*
      val behaviorWithSpv =
        Behaviors
          .supervise(
            Behaviors.supervise(actorRef).onFailure[RecoverableSinkFailure](akka.actor.typed.SupervisorStrategy.restart)
          )
          .onFailure[Throwable](akka.actor.typed.SupervisorStrategy.stop)

      //this method doesn't support bp so we use a rate limiting stage in front this sink, namely MergeHub.source[TypedProtocol](bufferSize)

      ActorSink.actorRef[DegradingTypedActorSink.Protocol](
        sys.spawn(behaviorWithSpv, name, akka.actor.typed.DispatcherSelector.fromConfig(FixedDispatcher)),
        DegradingTypedActorSink.OnCompleted,
        DegradingTypedActorSink.StreamFailure(_)
      )
       */

      ActorSink.actorRefWithAck[Int, DegradingTypedActorSinkPb.AckProtocol, DegradingTypedActorSinkPb.Ack](
        sys.spawn(DegradingTypedActorSinkPb(name, GraphiteMetrics(ms), 10, 0), name),
        DegradingTypedActorSinkPb.Next(_, _),
        DegradingTypedActorSinkPb.Init(_),
        DegradingTypedActorSinkPb.Ack,
        DegradingTypedActorSinkPb.Completed,
        DegradingTypedActorSinkPb.Failed(_)
      )
    }

    def actorBasedSink(n: Int): Sink[Int, NotUsed] =
      Sink.actorRefWithAck[Int](
        sys.actorOf(DegradingActorSink.props(s"akka-sink-7_2-$n", ms, 10)),
        onInitMessage = DegradingActorSink.Init,
        ackMessage = DegradingActorSink.Ack,
        onCompleteMessage = DegradingActorSink.OnCompleted,
        onFailureMessage = DegradingActorSink.StreamFailure(_)
      )

    def attachNSources(sink: Sink[Int, NotUsed], i: Int, limit: Int = 5): Future[Unit] = {
      val f = akka.pattern.after(5.second, sys.scheduler)(
        Future {
          val src =
            timedSource(ms, 0.millis, (i * 100).millis, (i * 10000) + numOfMsgPerSource, s"source_7_2-$i", i * 10000)
          println("attached source:" + i)
          src.to(sink).run()(mat)
          ()
        }
      )
      f.onComplete { _ ⇒
        if (i < limit) attachNSources(sink, i + 1, limit)
        else Future.successful(())
      }
      f
    }

    def attachNSinks(sourceH: Source[Int, NotUsed], i: Int, limit: Int = 5): Future[Unit] = {
      val f = akka.pattern.after(5.second, sys.scheduler)(Future {
        //println("attached sink:" + i)
        //actorBasedSink(i)
        sourceH.to(typedActorBasedSink(i)).run()(mat)
        ()
      })
      f.onComplete { _ ⇒
        if (i < limit) attachNSinks(sourceH, i + 1, limit)
        else Future.successful(())
      }
      f
    }

    val bufferSize = 1 << 4
    val (sinkHub, sourceHub) =
      MergeHub
        .source[Int](bufferSize) //If the consumer cannot keep up then all of the producers are back pressured.
        //.toMat(Sink.queue[Int]())(Keep.both)
        .toMat(BroadcastHub.sink[Int](bufferSize))(Keep.both) //The rate of the producers will be automatically set to the slowest consumer.
        .run()(mat)

    /*
      Ensure that the Broadcast output is dropped if there are no listening parties.
      If this dropping Sink is not attached, then the broadcast hub will not drop any
      elements itself when there are no subscribers, backpressuring the producer instead.
     */
    //sourceHub.runWith(Sink.ignore)(mat)

    //val gr = GraphiteMetrics(ms)
    attachNSources(sinkHub, 1, 3) //We can add new producers on the fly
    attachNSinks(sourceHub, 1, 3) //We can add new consumers on the fly
  }

  //Many-to-one
  def scenario7_3(mat: Materializer): Unit = {
    implicit val ec       = mat.executionContext
    val numOfMsgPerSource = 500

    val bufferSize = 1 << 6

    //https://doc.akka.io/docs/akka/current/stream/stream-dynamic.html#using-the-mergehub

    //If the consumer cannot keep up with the rate, all producers will be backpressured.
    val (sinkHub, queue) =
      MergeHub
        .source[Int](bufferSize)
        .toMat(Sink.queue[Int].addAttributes(Attributes.inputBuffer(bufferSize, bufferSize)))(Keep.both)
        .run()(mat)

    def attachSources(sink: Sink[Int, NotUsed], i: Int, limit: Int = 5): Future[Unit] = {
      val f = akka.pattern.after(3.second, sys.scheduler)(
        Future {
          val src =
            timedSource(ms, 0.millis, (i * 100).millis, (i * 10000) + numOfMsgPerSource, s"source_7_3-$i", i * 10000)
          println("attached source:" + i)
          src.to(sink).run()(mat)
          ()
        }
      )
      f.onComplete { _ ⇒
        if (i < limit) attachSources(sink, i + 1, limit)
        else Future.successful(())
      }
      f
    }

    def drain(q: SinkQueueWithCancel[Int]): Future[Unit] =
      q.pull.flatMap {
        case Some(r) ⇒
          println("sink_7_3 :" + r)
          drain(q)
        case None ⇒
          Future.successful(())
      }

    attachSources(sinkHub, 1, 3)
    drain(queue)
    ()
  }

  /**
    * Execute nested flows in parallel and merge results
    * Parallel fan-out fan-in
    */
  def scenario8: Graph[ClosedShape, akka.NotUsed] = {
    val latencies   = List(20L, 30L, 40L, 45L)
    val parallelism = latencies.size
    val bufferSize  = 128

    def action(sleep: Long) =
      Flow[Int].buffer(bufferSize, OverflowStrategy.backpressure).map { r ⇒
        Thread.sleep(sleep)
        r
      }

    def buffAttributes =
      Attributes.inputBuffer(initial = bufferSize, max = bufferSize)

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      //val balancer = b.add(new RoundRobinStage4[Int].withAttributes(buffAttributes))

      val source   = timedSource(ms, 1 second, 10 milliseconds, Int.MaxValue, "akka-source8")
      val sink     = Sink.actorSubscriber(SyncActor.props("akka-sink8", ms, 0L))
      val balancer = b.add(Balance[Int](parallelism, true))
      val merge    = b.add(Merge[Int](parallelism).withAttributes(buffAttributes))

      (source alsoTo countElementsWindow("akka-scenario8", 5 seconds)) ~> balancer

      latencies.zipWithIndex.foreach {
        case (l, ind) ⇒ balancer.out(ind).async ~> action(l) ~> merge
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
  def scenario08: Graph[ClosedShape, akka.NotUsed] = {
    val source    = timedSource(ms, 1 second, 100 milliseconds, Int.MaxValue, "akka-source-08")
    val errorSink = Sink.actorSubscriber(SyncActor.props("akka-sink-error08", ms, 1L)) //slow sink
    val sink      = Sink.actorSubscriber(SyncActor.props("akka-sink-08", ms, 0L))

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val router = b.add(new DisjunctionStage[Int, String]({ in: Int ⇒
        if (in % 10 == 0) Left(s"error element $in") else Right(in)
      }))

      source ~> router.in
      //errors
      router.out0 ~> Flow[String].buffer(1 << 5, OverflowStrategy.backpressure) ~> errorSink
      //results
      router.out1 ~> sink
      ClosedShape
    }
  }

  /**
    * A Fast source with conflate flow that buffer incoming message and produce single element
    */
  def scenario9: Graph[ClosedShape, akka.NotUsed] = {
    val source = timedSource(ms, 1 second, 10 milliseconds, Int.MaxValue, "akka-source9")
    val sink   = Sink.actorSubscriber(DegradingActor.props2("akka-sink9", ms, 0L))

    //conflate as buffer but without backpressure support
    def conflate0: Flow[Int, Int, akka.NotUsed] =
      Flow[Int]
        .conflateWithSeed(Vector(_))((acc, element) ⇒ acc :+ element)
        .mapConcat(identity)

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      ((source via Flow[Int].buffer(128, OverflowStrategy.backpressure)) via throttledFlow(100 milliseconds)) ~> sink
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
  def scenario9_1: Graph[ClosedShape, akka.NotUsed] = {
    val sink = Sink.actorSubscriber(DegradingActor.props2("akka-source9_1", ms, 0L))

    val aggregatedSource = timedSource(ms, 1 second, 200 milliseconds, Int.MaxValue, "akka-sink9_1")
      .scan(State(0L, 0L)) { (state, el) ⇒
        state.combine(el)
      }
      .conflateWithSeed(identity)(Keep.left)

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val src = timedSource(ms, 0 second, 200 milliseconds, Int.MaxValue, "akka-sink9_1")

      val broadcast = b.add(Broadcast[Int](2))
      val zip       = b.add(Zip[State, Int])

      val flow = Flow[Int]
        .buffer(64, OverflowStrategy.backpressure)
        .scan(State(0, 0)) { (state, el) ⇒
          state.combine(el)
        }
        .conflateWithSeed(identity)(Keep.left)

      val window = 1000 milliseconds

      src ~> broadcast ~> flow ~> zip.in0
      broadcast ~> Flow[Int].dropWithin(window) ~> zip.in1
      zip.out ~> sink

      //src ~> (flow via throttledFlow(1000 milliseconds)) ~> sink
      //(aggregatedSource via throttledFlow(1000 milliseconds)) ~> sink
      ClosedShape
    }
  }

  case class State(count: Long, sum: Long) {
    def combine(current: Long): State = this.copy(this.count + 1, this.sum + current)
  }

  def every[T](interval: FiniteDuration): Flow[T, T, akka.NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val zip     = b.add(ZipWith[T, Unit, T](Keep.left).withAttributes(Attributes.inputBuffer(1, 1)))
        val dropOne = b.add(Flow[T].drop(1))

        Source.tick(Duration.Zero, interval, ()) ~> zip.in1
        zip.out ~> dropOne.in
        FlowShape(zip.in0, dropOne.outlet)
      }
    )

  /**
    * Almost same as ``every``
    */
  def throttledFlow[T](interval: FiniteDuration): Flow[T, T, akka.NotUsed] =
    Flow
      .fromGraph(
        GraphDSL.create() { implicit b ⇒
          import GraphDSL.Implicits._
          val zip = b.add(Zip[T, Unit]().withAttributes(Attributes.inputBuffer(1, 1)))
          Source.tick(interval, interval, ()) ~> zip.in1
          FlowShape(zip.in0, zip.out)
        }
      )
      .map(_._1)

  /**
    * Fast sink and heartbeats sink.
    * Sink's rate is equal to sum of 2 sources
    *
    */
  def scenario10: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val source = timedSource(ms, 1 second, 20 milliseconds, Int.MaxValue, "akka-source10")
      val sink   = Sink.actorSubscriber(DegradingActor.props2("akka-sink10", ms, 0L))
      source ~> heartbeats(50.millis, 0) ~> sink
      ClosedShape
    }

  def heartbeats[T](interval: FiniteDuration, zero: T): Flow[T, T, akka.NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() { implicit builder ⇒
        import GraphDSL.Implicits._
        val heartbeats = builder.add(Source.tick(interval, interval, zero))
        //0 - preferred port
        //1 - secondary port
        val merge = builder.add(MergePreferred[T](1))
        heartbeats ~> merge.in(0)
        FlowShape(merge.preferred, merge.out)
      }
    )

  /*
    For cases where back-pressuring is not a viable strategy, one may wants to drop events from the fast producer, or accumulate them
    while waiting for the slow producer, or vice versa interpolate the output of the slow producer to cope with the fast one.
    This can be done with the conflate and expand operations.
    The conflate operator allows us to fold elements of a fast producer attached to a slow consumer.
    For instance, dropping every event except for the last one would be just :
      val skipped = fastProducer.conflate(identity)((oldMsg, newMsg) => newMsg)

    On the other side, one can use expand to cope with request from a consumer that is faster than we are producing.
    For instance, plain repetition is just:
      val repeatedStream = slowProducer.expand(identity)(s => (s, s))


    More complex cases can be handled by defining custom processing stages.
    This can be done by extending an abstract class called GraphStage.
   */

  //Detached flows with conflate + conflate
  def scenario11: Graph[ClosedShape, akka.NotUsed] = {
    val srcSlow = timedSource(ms, 1 second, 1000 milliseconds, Int.MaxValue, "akka-source11_0")
      .conflateWithSeed(identity)(_ + _)

    val srcFast =
      timedSource(ms, 1 second, 200 milliseconds, Int.MaxValue, "akka-source11_1")
        .conflateWithSeed(identity)(_ + _)

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink11", ms, 0L))
      ClosedShape
    }
  }

  //Detached flows with expand + conflate
  def scenario12: Graph[ClosedShape, akka.NotUsed] = {
    val srcFast = timedSource(ms, 1 second, 200 milliseconds, Int.MaxValue, "akka-source12_1")
      .conflate(_ + _)
    val srcSlow = timedSource(ms, 1 second, 1000 milliseconds, Int.MaxValue, "akka-source12_0")
      .expand(Iterator.continually(_))

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val zip = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(16, 32)))
      srcFast ~> zip.in0
      srcSlow ~> zip.in1
      zip.out ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink12", ms, 0L))
      ClosedShape
    }
  }

  /**
    * External source
    * In 2.0 for this purpose you should can use  [[Source.queue.offer]]
    */
  def scenario13(mat: ActorMaterializer): Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      //no backpressure there, just dropping
      val (actor, publisher) = Source
        .actorRef[Int](200, OverflowStrategy.dropTail)
        .toMat(Sink.asPublisher[Int](false))(Keep.both)
        .run()(mat)

      val i = new AtomicInteger()
      sys.scheduler.schedule(1 second, 20 milliseconds) {
        i.getAndIncrement()
        if (i.get() < Int.MaxValue) actor ! i
      }(mat.executionContext)

      Source.fromPublisher(publisher) ~> Sink.actorSubscriber(DegradingActor.props2("akka-sink13", ms, 13L))
      ClosedShape
    }

  /**
    * External Producer through Source.queue
    */
  def scenario13_1(
    implicit
    mat: Materializer
  ): Graph[ClosedShape, akka.NotUsed] = {
    implicit val Ctx    = mat.executionContext
    implicit val ExtCtx = sys.dispatchers.lookup("akka.blocking-dispatcher")

    val pubStatsD = new GraphiteMetrics {
      override val address = ms
    }

    // If you want to get a queue as a source
    val (queue, publisher) = Source
      .queue[Int](1 << 7, OverflowStrategy.backpressure)
      .toMat(Sink.asPublisher[Int](false))(Keep.both)
      .run()(mat)

    /*
    If you want to get an actor as a source
    val (actor, publisher) = Source.actorRef[Int](1 << 7, OverflowStrategy.backpressure)
      .toMat(Sink.asPublisher[Int](false))(Keep.both).run()(mat)
     */

    def externalProducer(q: akka.stream.scaladsl.SourceQueueWithComplete[Int], pName: String, elem: Int): Unit =
      if (elem < 10000) {
        (q offer elem).onComplete {
          case Success(QueueOfferResult.Enqueued) ⇒
            (pubStatsD send pName)
            externalProducer(q, pName, elem + 1)
          case Failure(ex) ⇒
            println(s"error: elem $elem error" + ex.getMessage)
            sys.scheduler.scheduleOnce(1 seconds)(externalProducer(q, pName, elem))(ExtCtx) //retry
        }(ExtCtx)
      } else {
        println("External-producer is completed")
        q.complete()
        q.watchCompletion()
          .onComplete { _ ⇒
            println("watchCompletion")
          }(ExtCtx)
      }

    externalProducer(queue, "source_13_1:1|c", 0)

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      (Source.fromPublisher(publisher) alsoTo countElementsWindow("akka-scenario13_1", 3 seconds)) ~> Sink
        .actorSubscriber(DegradingActor.props2("akka-sink_13_1", ms, 1L))
      ClosedShape
    }
  }

  /**
    * Batched source with external effect as an Actor through Flow and degrading sink
    * The whole pipeline is going to slow down up to sink's rate
    * http://fehmicansaglam.net/connecting-dots-with-akka-stream/
    *
    * In this scenario let's assume that we are reading a batch of items from an internal system,
    * making a request for each item to an external service,
    * then sending an event to a stream (e.g. Kafka)
    * for each item received from the external service
    */
  def scenario14: Graph[ClosedShape, akka.NotUsed] = {
    val batchedSource = Source.actorPublisher[Vector[Item]](BatchProducer.props)
    val sink          = Sink.actorSubscriber[Int](DegradingActor.props2("akka-sink14", ms, 10L))
    val external      = Flow[Item].buffer(1, OverflowStrategy.backpressure).map(_.num)

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
    * Parallel
    * +------+
    * +--|Worker|--+
    * |  +------+  |
    * +-----------------+     +--------------+      |  +------+  |  +-----------+
    * |DbCursorPublisher|-----|BalancerRouter|------|--|Worker|-----|RecordsSink|
    * +-----------------+     +--------------+      |  +------+  |  +-----------+
    * |  +------+  |
    * +--|Worker|--+
    * +------+
    */
  def scenario15: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val out = sys.actorOf(
        Props(classOf[RecordsSink], "sink15", ms)
          .withDispatcher("akka.flow-dispatcher"),
        "akka-sink15"
      )

      val src = Source
        .actorPublisher[Long](
          Props(classOf[DbCursorPublisher], "akka-source15", 20000L, ms)
            .withDispatcher("akka.flow-dispatcher")
        )
        .map(DBObject(_, out))
      src ~> Sink.actorSubscriber(BalancerRouter.props)
      ClosedShape
    }

  //
  import akka.stream.SourceShape
  import akka.stream.Graph
  import akka.stream.stage.GraphStage
  import akka.stream.stage.OutHandler
  import akka.stream.SinkShape
  import akka.stream.stage.GraphStage
  import akka.stream.stage.InHandler

  class NumbersSource extends GraphStage[SourceShape[Int]] {
    val out: Outlet[Int]                 = Outlet("NumbersSource")
    override val shape: SourceShape[Int] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        // All state MUST be inside the GraphStageLogic,
        // never inside the enclosing GraphStage.
        // This state is safe to access and modify from all the
        // callbacks that are provided by GraphStageLogic and the
        // registered handlers.
        private var counter = 1

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            push(out, counter)
            counter += 1
          }
        })
      }
  }

  class StdoutSink extends GraphStage[SinkShape[Int]] {
    val in: Inlet[Int]                 = Inlet("StdoutSink")
    override val shape: SinkShape[Int] = SinkShape(in)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        override def preStart(): Unit = pull(in)

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            println(grab(in))
            pull(in)
          }
        })
      }
  }

  //akka 2.5
  def scenario15_001 = {
    val sourceGraph: Graph[SourceShape[Int], akka.NotUsed] = new NumbersSource
    val mySource: Source[Int, akka.NotUsed]                = Source.fromGraph(sourceGraph)
    //val r: Future[Int] = mySource.take(10).runFold(0)(_ + _)
    val stdSink = new StdoutSink
    (mySource to stdSink)
  }

  def scenario15_01: Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._

      val sink =
        sys.actorOf(Props(classOf[RecordsSink], "sink15_01", ms).withDispatcher("akka.flow-dispatcher"), "akka-sink15")

      val src = Source
        .actorPublisher[Long](
          Props(classOf[DbCursorPublisher], "akka-source15_01", 20000L, ms).withDispatcher("akka.flow-dispatcher")
        )
        .map(DBObject2(_, sink))

      src ~> Sink.actorSubscriber(ConsistentHashingRouter.props)
      ClosedShape
    }

  def tailer(path: String, n: Int)(implicit ex: ExecutionContext): Source[String, akka.NotUsed] = {
    val delimeter = Framing.delimiter(ByteString("\n"), maximumFrameLength = 10000, allowTruncation = true)
    val proc      = new java.lang.ProcessBuilder().command("tail", s"-n$n", "-f", path).start()
    proc.getOutputStream.close()
    val input = proc.getInputStream

    def readChunk(): scala.concurrent.Future[ByteString] = Future {
      val buffer = new Array[Byte](1024 * 6)
      val read   = (input read buffer)
      println(s"available: $read")
      if (read > 0) ByteString.fromArray(buffer, 0, read) else ByteString.empty
    }

    val publisher = Source
      .repeat(0)
      .mapAsync(1)(_ ⇒ readChunk())
      .takeWhile(_.nonEmpty)
      .via(delimeter)
      /*
      .transform(() ⇒
        new PushStage[ByteString, ByteString] {
          override def onPush(elem: ByteString, ctx: Context[ByteString]): SyncDirective =
            ctx.push(elem ++ ByteString('\n'))

          override def postStop(): Unit = {
            println("tailing  has been finished")
            proc.destroy()
          }
        })*/
      .runWith(Sink.asPublisher(true))(mat)

    Source.fromPublisher(publisher).map(_.utf8String)
  }

  /**
    * 2 subscribers for single file from tail
    * The first one faster then the second one.
    * If the first subscriber beats the second by 32 elements he will be backpressured.
    */
  def scenario16(mat: ActorMaterializer): Graph[ClosedShape, akka.NotUsed] = {
    val log         = "./example.log"
    val n           = 50
    implicit val ec = mat.executionContext

    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[String](2))
      tailer(log, n) ~> broadcast ~> Sink.actorSubscriber[String](SyncActor.props4("akka-sink16_0", ms, 500L, n))
      broadcast ~> Flow[String].buffer(32, OverflowStrategy.backpressure) ~> Sink.actorSubscriber[String](
        SyncActor.props4("akka-sink16_1", ms, 1000L, n)
      )
      ClosedShape
    }
  }

  /**
    *
    *
    */
  def scenario17(): RunnableGraph[Future[IOResult]] = {
    import GeoJsonProtocol._
    import spray.json._
    type Histogram = Map[String, Long]

    val delimiter = Framing.delimiter(ByteString('\n'), Int.MaxValue, true)

    def table(line: String) =
      new StringBuilder()
        .append("\n")
        .append("*****************")
        .append("\n")
        .append(line)
        .append("\n")
        .append("*****************")
        .toString()

    def borough(features: IndexedSeq[Feature], point: Point) =
      features
        .find(_.geometry.contains(point))
        .map(_("borough").convertTo[String])

    def featuresMap(): IndexedSeq[Feature] = {
      val src = scala.io.Source.fromFile("nyc-borough-boundaries-polygon.geojson")
      val geo = src.mkString
      src.close()
      geo.parseJson.convertTo[Features].sortBy { f ⇒
        val borough = f("boroughCode").convertTo[Int]
        (borough, -f.geometry.area2D())
      }
    }

    val areaMap = featuresMap()

    def updateHistogram(hist: Histogram, region: Option[String]) =
      region.fold(hist) { r ⇒
        hist.updated(r, hist.getOrElse(r, 0L) + 1)
      }

    val src = (StreamConverters.fromInputStream(() ⇒ new FileInputStream("./taxi.log")) via delimiter)
      .map { line ⇒
        val fields = line.utf8String.split(",")
        borough(areaMap, new Point(fields(10).toDouble, fields(11).toDouble))
      }
      .scan[Histogram](Map.empty)(updateHistogram)
      .map(_.toVector.sortBy(-_._2))

    val sink = Sink.actorSubscriber[String](SyncActor.props4("akka-sink_17", ms, 500L, 5000))

    val printFlow = Flow[Vector[(String, Long)]]
      .buffer(1, OverflowStrategy.backpressure)
      .map(vector ⇒ table(vector.mkString("\n")))

    (src via printFlow to sink)
  }

  case class LogEntry(ts: Long, message: String)

  /**
    * We are replaying log with ts attached to every line
    * This emits lines according to a time that is derived from the message itself.
    */
  def scenario18(): Graph[ClosedShape, akka.NotUsed] = {
    val iter = Iterator.iterate(LogEntry(1000L, Thread.currentThread.getName)) { log ⇒
      println(log.message)
      //ts in logEntry grows monotonically
      log.copy(ts = log.ts + ThreadLocalRandom.current.nextLong(1000L, 3000L))
    }
    val logEntries = Source.fromIterator(() ⇒ iter)

    val ratedSource = new TimeStampedLogReader[LogEntry](_.ts)
    val sink        = Sink.actorSubscriber[LogEntry](SyncActor.props2("akka-sink_18", ms))

    //We use asyncBoundary here so that source can use blocking-dispatcher
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      (logEntries via ratedSource)
        .withAttributes(Attributes.asyncBoundary)
        .withAttributes(ActorAttributes.dispatcher("akka.blocking-dispatcher")) ~> sink
      ClosedShape
    }
  }

  def scenario19(): Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val sink = Sink.actorSubscriber(SyncActor.props2("akka-sink_19", ms))
      val src  = timedSource(ms, 1 second, 900 milliseconds, Int.MaxValue, "akka-source_19")
      src ~> trailingDifference(4) ~> sink
      ClosedShape
    }

  case class SimpleMAState[T: SourceElement: ClassTag: Numeric] private (
    ma: Double,
    capacity: Int,
    buffer: SimpleRingBuffer[T]
  ) {
    //@specialized(Int, Long, Double) T: scala.reflect.ClassTag: Fractional](ma: T,
    //import scala.collection.JavaConverters._
    //new CircularFifo[Int](lenght)

    val d = implicitly[SourceElement[T]]

    def this(capacity: Int) = {
      this(.0, capacity, new SimpleRingBuffer[T](capacity))
    }

    def :+(element: T): SimpleMAState[T] = {
      val leavingElem = buffer.currentHead
      buffer.add(element)
      if (buffer.size < capacity) this
      else if (ma == .0) copy(ma = buffer.sum / capacity)
      else copy(ma = ma + (d(element) - d(leavingElem)) / capacity)
    }

    override def toString = s"ma: $ma"
  }

  /**
    * Sliding window on bounded memory with CircularFifoQueue, drops elements storing only last 5
    */
  def scenario20(): Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val lenght = 5
      val sink   = Sink.actorSubscriber(SyncActor.props("akka-sink_20", ms, 10L))
      val src    = timedSource(ms, 1 second, 300 milliseconds, Int.MaxValue, "akka-source_20")

      val a = Flow[Int].conflateWithSeed({ e ⇒
        val fifo = new CircularFifo[Int](lenght)
        fifo.add(e)
        fifo
      }) { (fifo, e) ⇒
        fifo.add(e)
        fifo
      }

      val slidingWindow =
        a.zipWith(Source.tick(1 seconds, 1 seconds, ()))(Keep.left)
          .withAttributes(Attributes.inputBuffer(1, 1))

      src ~> slidingWindow ~> sink
      ClosedShape
    }

  /**
    *
    * Sliding window on bounded memory with CircularFifoQueue, doesn't drop elements
    */
  def scenario21(): Graph[ClosedShape, akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val lenght = 5
      val sink   = Sink.actorSubscriber(SyncActor.props("akka-sink_21", ms, 10L))
      val src    = timedSource(ms, 1 second, 200 milliseconds, Int.MaxValue, "akka-source_21")

      val simpleMovingAverage = Flow[Int]
        .buffer(lenght, OverflowStrategy.backpressure)
        .map(_.toLong)
        .scan(new SimpleMAState[Long](lenght)) { (state, next) ⇒
          state.:+(next)
        }

      val slidingWindow = Flow[Int]
        .buffer(lenght, OverflowStrategy.backpressure)
        .scan(new CircularFifo[Int](lenght)) { (q, e) ⇒
          q.add(e)
          q
        }

      src ~> simpleMovingAverage ~> sink
      ClosedShape
    }

  /*def scenario22(implicit sys: ActorSystem): Graph[ClosedShape, akka.NotUsed] = {
    implicit val serializer = org.squbs.pattern.stream.QueueSerializer[Int]()
    //val degradingSink = new DegradingGraphiteSink[org.squbs.pattern.stream.Event[Int]]("sink_22", 2l, ms)

    val dbFlow = Flow[org.squbs.pattern.stream.Event[Int]]
      .buffer(1, OverflowStrategy.backpressure)
      .map { e ⇒
        Thread.sleep(200) //save to db
        println(s"${Thread.currentThread.getName} save ${e.entry} - ${e.index}")
        e
      }

    //https://github.com/paypal/squbs/blob/master/docs/persistent-buffer.md

    /*
      It works like the Akka Streams buffer with the difference that the content of the buffer is stored in a series of memory-mapped files
      in the directory given at construction of the PersistentBuffer. This allows the buffer size to be virtually limitless,
      not use the JVM heap for storage, and have extremely good performance in the range of a million messages/second at the same time.
   */

    //IDEA: to use PersistentBuffer as a commit log
    val file    = new File("/Volumes/dev/github/streams-recipes/pqueue")
    val pBuffer = new org.squbs.pattern.stream.PersistentBufferAtLeastOnce[Int](file)
    val commit  = pBuffer.commit[Int]
    val src     = timedSource(ms, 1 second, 50 milliseconds, Int.MaxValue, "akka-source_22")

    /*val (queue, publisher) = Source
      .queue[Int](1 << 7, OverflowStrategy.backpressure)
      .toMat(Sink.asPublisher[Int](false))(Keep.both)
      .run()(mat)*/

    //read the latest saved date form DB and fetch the next page
    //queue.offer()

    //Source.fromPublisher(publisher)
    src //.alsoTo(allWindow("akka-scenario22", 5 seconds))
      .via(pBuffer.async)
      //.mapAsync(2) { e => /*ask*/ }
      .via(dbFlow.async("akka.blocking-dispatcher")) //AtLeastOnce so the writes should be idempotent
      .via(commit)
      .to(Sink.ignore)
  }*/

  def scenario23(implicit sys: ActorSystem): Graph[ClosedShape, akka.NotUsed] = {
    val src     = timedSource(ms, 1 second, 15 milliseconds, Int.MaxValue, "akka-source_23")
    val sinkRef = sys.actorOf(DegradingActorSink.props("akka-sink_23", ms, 10))

    //backpressure with one by one semantic
    val sink = Sink.actorRefWithAck(
      sinkRef,
      onInitMessage = DegradingActorSink.Init,
      ackMessage = DegradingActorSink.Ack,
      onCompleteMessage = DegradingActorSink.OnCompleted,
      onFailureMessage = DegradingActorSink.StreamFailure(_)
    ): Sink[Int, NotUsed]
    val ks = KillSwitches.shared("kw")
    //ks.shutdown()
    src
    //.alsoTo(tumblingWindow("akka-scenario23", 10 seconds))
      .alsoTo(slidingWindow("akka-scenario23", 7 seconds, 3))
      .via(Flow[Int].buffer(1 << 6, OverflowStrategy.backpressure) /*.async("akka.blocking-dispatcher")*/ )
      .via(ks.flow)
      .to(sink)
  }

  def scenario24: Graph[ClosedShape, akka.NotUsed] = {
    val window = 5
    val src    = timedSource(ms, 1.second, 1.seconds, Int.MaxValue, "akka-source_24")
    src
      .map { i ⇒
        (i.toDouble, .0)
      }
      .via(DelayFlow(window, .1).filter(_._1 > 0)) //ignore first window
      //.via(DelayFlow(window, .5).filter(_._1 > 0))
      .to(new GraphiteSink[(Double, Double)]("sink_24", 0, ms))
  }

  def scenario25: Graph[ClosedShape, akka.NotUsed] = {
    val src = timedSource(ms, 1.second, 1.seconds, Int.MaxValue, "akka-source_25", start = 1)
    src
      .map(_.toDouble)
      .via(TrailingDifference[Double](5).drop(5)) //ignore first window
      .to(new GraphiteSink[Double]("sink_25", 0, ms))
  }

  def scenario26: Graph[ClosedShape, akka.NotUsed] = {
    val src = timedSource(ms, 1.second, 1.seconds, Int.MaxValue, "akka-source_26", start = 1)
    src
      .via(MovingAvg[Int](8))
      .to(new GraphiteSink[Double]("sink_26", 0, ms))
  }

  case class Handler[T](handler: Try[T])

  def scenario27(): RunnableGraph[SourceQueueWithComplete[Future[Handler[Int]]]] = {
    def queueGraph[T](
      onBatch: List[Handler[T]] ⇒ Future[Unit],
      parallelism: Int = 3
    ): RunnableGraph[SourceQueueWithComplete[Future[Handler[T]]]] =
      Source
        .queue[Future[Handler[T]]](1 << 6, OverflowStrategy.backpressure)
        .mapAsync(parallelism)(identity)
        .batch[List[Handler[T]]](1 << 3, x ⇒ List(x)) { (xs, x) ⇒
          x :: xs
        }
        .to(Sink.foreachAsync(parallelism)(xs ⇒ onBatch(xs.reverse)))
    //or
    //.mapAsync(parallelism)(xs ⇒ onBatch(xs.reverse)).to(Sink.ignore)

    def onBatch[T](batch: List[Handler[T]]): Future[Unit] = {
      //tell self
      val promise = Promise[Unit]

      val max = FiniteDuration(ThreadLocalRandom.current.nextInt(2000), MILLISECONDS)
      sys.scheduler.scheduleOnce(max) { promise.success(()) }

      val f = promise.future
      f.onComplete(_ ⇒ println(s"${Thread.currentThread.getName}: onBatchComplete - ${batch.mkString(",")} "))
      f
    }

    queueGraph[Int](onBatch[Int])
  }

  //global rate limit
  def scenario28() = {
    //val process: Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Http.HostConnectionPool]

    def process: FlowWithContext[HttpRequest, Promise[HttpResponse], HttpResponse, Promise[HttpResponse], Any] =
      FlowWithContext[HttpRequest, Promise[HttpResponse]]
        .withAttributes(Attributes.inputBuffer(1, 1))
        .mapAsync(2) { req: HttpRequest ⇒
          Future { null.asInstanceOf[HttpResponse] }
        }
    //.map { req: HttpRequest =>  null.asInstanceOf[HttpResponse] }

    Source
      .queue[(HttpRequest, Promise[HttpResponse])](1 << 5, OverflowStrategy.dropNew)
      .via(process) //CachedHttpClient(context.system)
      .withAttributes(ActorAttributes.supervisionStrategy(akka.stream.Supervision.resumingDecider))
      .toMat(Sink.foreach {
        case (resp, p) ⇒ p.success(resp)
        //case (Success(resp), p) => p.success(resp)
        //case (Failure(e), p) => p.failure(e)
      })(Keep.left)
    //.run()

    //queue.offer(null.asInstanceOf[HttpRequest])
  }

  type IN = (HttpRequest, Promise[HttpResponse])

  def scenario29() = {

    type CachedClient =
      Flow[IN, (Try[HttpResponse], Promise[HttpResponse]), Http.HostConnectionPool]
    //FlowWithContext[HttpRequest, Promise[HttpResponse], HttpResponse, Promise[HttpResponse], Any]
    //Flow[P, (Try[HttpResponse], Promise[HttpResponse]), Http.HostConnectionPool]

    val queueSize = 1 << 5

    val fallback: Graph[SourceShape[(Try[HttpResponse], Promise[HttpResponse])], akka.NotUsed] =
      GraphDSL.create() { implicit b ⇒
        import scala.concurrent.duration._
        val data: (Try[HttpResponse], Promise[HttpResponse]) = ???
        val flow                                             = b.add(Source.tick(1.seconds, 1.seconds, data))
        SourceShape(flow.out)
      }

    /**
      * https://doc.akka.io/docs/akka-http/current/client-side/host-level.html#using-the-host-level-api-with-a-queue
      */
    val client: CachedClient = ???

    /**
      * Queue for our internal http client that is used before opening new web socket connection
      * If too many requests flow will fail,
      * If too many open connections then it will start to reject incoming connections.
      */
    val queue =
      Source
        .queue[IN](queueSize, OverflowStrategy.fail)
        .via(client)
        .recoverWithRetries(3, { case ex: Throwable ⇒ fallback })
        .toMat(Sink.foreach {
          case (Success(resp), p) ⇒ p.success(resp)
          case (Failure(e), p)    ⇒ p.failure(e)
        })(Keep.left)
        .run()(???)

    def enqueueRequest(request: HttpRequest): Future[HttpResponse] = {
      val responsePromise = Promise[HttpResponse]
      queue.offer(request → responsePromise).flatMap {
        case QueueOfferResult.Enqueued ⇒
          responsePromise.future
        case QueueOfferResult.Dropped ⇒
          throw new Exception("Internal http client overflowed")
        case QueueOfferResult.Failure(ex) ⇒
          throw ex
        case QueueOfferResult.QueueClosed ⇒
          throw new Exception("Internal http client pool shutted down")
      }
    }
  }

  def scenario30 = {
    type P = (Int, Promise[Int])

    def out(
      queue: SinkQueueWithCancel[P]
    ): Source[P, akka.NotUsed] =
      Source.unfoldAsync[SinkQueueWithCancel[P], P](queue) { q: SinkQueueWithCancel[P] ⇒
        q.pull()
          .map(_.map(el ⇒ (q, el)))
      }

    def process(
      sink: Sink[(Int, Promise[Int]), akka.NotUsed]
    ): FlowWithContext[Int, Promise[Int], Int, Promise[Int], Any] =
      FlowWithContext[Int, Promise[Int]]
        .withAttributes(Attributes.inputBuffer(1, 4))
        .mapAsync(2) { in: Int ⇒
          //Feed stuff into remotable stream
          Source
            .single(in)
            .map { i ⇒
              (i, Promise[Int])
            }
            .alsoTo(Flow[(Int, Promise[Int])].to(sink))
            .mapAsync(1) { _._2.future }
            .runWith(Sink.head)(mat)
        }
        .map(identity)

    val ((sink, killSwitch), src) =
      MergeHub
        .source[(Int, Promise[Int])](perProducerBufferSize = 4)
        .viaMat(KillSwitches.single)(Keep.both)
        .toMat(Sink.queue[(Int, Promise[Int])])(Keep.both)
        //.addAttributes(Attributes.inputBuffer(bufSize.initial, bufSize.max)))(Keep.both)
        //.toMat(BroadcastHub.sink[(E, CtxOut)])(Keep.both)
        .run()(mat)

    val flow = process(sink).asFlow
      .alsoTo(
        Flow[Any]
          .to(Sink.onComplete {
            case Success(_)     ⇒ killSwitch.shutdown()
            case Failure(cause) ⇒ killSwitch.abort(cause)
          })
      )
      .merge(out(src))

    FlowWithContext.fromTuples(flow)
    ???
  }

  def scenario31 = {

    def qOut[P](queue: SinkQueueWithCancel[P]): Source[P, akka.NotUsed] =
      Source.unfoldAsync[SinkQueueWithCancel[P], P](queue)(q ⇒ q.pull.map(_.map(el ⇒ (q, el))))

    def qPublisher[T](queue: SinkQueueWithCancel[T]): Publisher[T] =
      Source
        .repeat(0)
        .mapAsync(1)(_ ⇒ queue.pull)
        .takeWhile(_.nonEmpty)
        .map(_.get)
        .runWith(Sink.asPublisher(false))(mat)

    //timedSource(ms, 1.second, 1.seconds, Int.MaxValue, "akka-source_31_0", start = 100).to(sink).run()(mat)
    //timedSource(ms, 1.second, 2.seconds, Int.MaxValue, "akka-source_31_1", start = 200).to(sink).run()(mat)
    //timedSource(ms, 1.second, 3.seconds, Int.MaxValue, "akka-source_31_2", start = 300).to(sink).run()(mat)

    //Source.fromPublisher(qPublisher(src)).runWith(Sink.foreach(println(_)))(mat)
    //qOut(src).runWith(Sink.foreach(println(_)))(mat)

    //Use case: Many clients write at most once (no response required).
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val bufferSize = 1 << 4

      //https://doc.akka.io/docs/akka/current/stream/stream-dynamic.html#using-the-mergehub
      //val ((sink, killSwitch), src) =
      /*
        A MergeHub is a special streaming hub that is able to collect streamed elements from a dynamic set of producers.
        It consists of two parts, a Source and a Sink. The Source streams the element to a consumer from its merged inputs.
        Once the consumer has been materialized, the Source returns a materialized value which  is the corresponding Sink.
        This Sink can then be materialized arbitrary many times, where each of the new materializations will feed its consumed elements to the  original Source.

        If the consumer cannot keep up with the rate, all producers will be backpressured.
       */

      /*
      .via(new InternalBufferStage[Int](1 << 7))
            .async
            .to(sink)
       */

      val (sink, publisher) =
        MergeHub
          .source[Int](perProducerBufferSize = 4)
          //insert a buffer stage to decouple the downstream from the MergeHub. If/when the buffer fulls up and a new element arrives, it drops the new element.
          //.via(Flow[Int].buffer(bufferSize, OverflowStrategy.dropNew).async(FixedDispatcher))
          //.via(new BackPressuredStage[Int](bufferSize).async(FixedDispatcher))
          .via(new DropTailStage[Int](bufferSize).async(FixedDispatcher))
          //.via(new DropHeadStage[Int](bufferSize).async(FixedDispatcher))
          .via(new BackpressureMeasurementStage)
          /*.via(
            Flow[Int]
              .map { i ⇒
                //println(s"${Thread.currentThread.getName}: Buffer:$i")
                i
              }
              .async(FixedDispatcher, bufferSize)
          )*/
          //.via(Flow[Int].addAttributes(Attributes.inputBuffer(bufferSize, bufferSize)).addAttributes(Attributes.asyncBoundary))
          .toMat(Sink.asPublisher[Int](false))(Keep.both)
          //.addAttributes(Attributes.inputBuffer(bufferSize, bufferSize))
          //.addAttributes(Attributes.asyncBoundary) //to improve the performance we apply async boundaries so that the
          //.toMat(Sink.queue[Int])(Keep.both)
          .run()(mat)

      //materialize this sink 3 times and each of the new materializations will feed its consumed elements to the original Source.
      timedSource(ms, 1.second, 10.millis, Int.MaxValue, "akka-source_31_0", start = 1000000) ~> sink
      timedSource(ms, 1.second, 12.millis, Int.MaxValue, "akka-source_31_1", start = 2000000) ~> sink
      //timedSource(ms, 1.second, 10.millis, Int.MaxValue, "akka-source_31_2", start = 3000000) ~> sink

      Source.fromPublisher(publisher) ~> new DegradingGraphiteSink[Int]("akka-sink_31", 1L, ms)

      /*Sink.foreach { i: Int ⇒
        println(s"${Thread.currentThread.getName}: Out:$i")
      }*/

      /*Source.fromGraph(new QueueSrc(src)) ~> Sink.foreach { i: Int ⇒
        println(s"out: $i")
      }*/

      /*qOut(src) ~> Sink.foreach { i: Int ⇒
        println(s"out: $i")
      }*/

      /*Source.fromPublisher(qPublisher(src)) ~> Sink.foreach { i: Int ⇒
        println("out:" + i)
      }*/

      ClosedShape
    }
  }

  //http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/
  def trailingDifference(offset: Int): Graph[FlowShape[Int, Int], akka.NotUsed] =
    GraphDSL.create() { implicit b ⇒
      import GraphDSL.Implicits._
      val broadcast = b.add(Broadcast[Int](2))
      val zip       = b.add(Zip[Int, Int].withAttributes(Attributes.inputBuffer(1, 1)))

      val processing = b.add(Flow[(Int, Int)].map(nums ⇒ nums._1 - nums._2))

      broadcast ~> Flow[Int].buffer(offset, akka.stream.OverflowStrategy.backpressure) ~> zip.in0
      broadcast ~> Flow[Int].drop(offset) ~> zip.in1
      zip.out ~> processing

      FlowShape(broadcast.in, processing.outlet)
    }

  /**
    * Create a source which is throttled to a number of message per second.
    */
  def timedSource(
    statsD: InetSocketAddress,
    delay: FiniteDuration,
    interval: FiniteDuration,
    limit: Int,
    name: String,
    start: Int = 0
  ): Source[Int, akka.NotUsed] =
    Source.fromGraph(
      GraphDSL.create() { implicit b ⇒
        import GraphDSL.Implicits._
        val sendBuffer = ByteBuffer.allocate(1024)
        val channel    = DatagramChannel.open()

        /*Flow[Int].mapAsync(1) { i =>
          akka.pattern.after(100.millis, sys.scheduler)(Future.successful(i))
        }*/

        // two sources
        val tickSource = Source.tick(delay, interval, ())
        val dataSource = Source.fromIterator(() ⇒ Iterator.range(start, limit))

        def send(message: String) = {
          sendBuffer.put(message getBytes "utf-8")
          sendBuffer.flip
          channel.send(sendBuffer, statsD)
          sendBuffer.limit(sendBuffer.capacity)
          sendBuffer.rewind
        }

        val sendOut = b.add(Flow[Int].map { x ⇒
          send(s"$name:1|c")
          x
        })

        // we use zip to throttle the stream
        val zip   = b.add(Zip[Unit, Int]())
        val unzip = b.add(Flow[(Unit, Int)].map(_._2))

        // setup the message flow
        tickSource ~> zip.in0
        dataSource ~> zip.in1

        zip.out ~> unzip ~> sendOut

        SourceShape(sendOut.outlet)
      }
    )

  //Streams first right element. Recurse on each left element
  def tailRecM[A, B](a: A)(f: A ⇒ Source[Either[A, B], NotUsed]): Source[B, NotUsed] =
    f(a).flatMapConcat {
      case Right(a)    ⇒ Source.fromIterator(???) //single(a)
      case Left(nextA) ⇒ tailRecM(nextA)(f)
    }
}

object GraphiteMetrics {

  def apply(address0: InetSocketAddress) =
    new GraphiteMetrics() {
      override val sendBuffer = ByteBuffer.allocate(1 << 7)
      override val address    = address0

      override def send(msg: String) = {
        sendBuffer.put(msg.getBytes(Encoding))
        sendBuffer.flip
        channel.send(sendBuffer, address)
        sendBuffer.limit(sendBuffer.capacity)
        sendBuffer.rewind
      }
    }
}

trait GraphiteMetrics {
  val Encoding   = "utf-8"
  val sendBuffer = ByteBuffer.allocate(512)
  val channel    = DatagramChannel.open()

  def address: InetSocketAddress

  def send(message: String) = {
    sendBuffer.put(message.getBytes(Encoding))
    sendBuffer.flip
    channel.send(sendBuffer, address)
    sendBuffer.limit(sendBuffer.capacity)
    sendBuffer.rewind
  }
}

object BalancerRouter {

  case class DBObject(id: Long, replyTo: ActorRef)

  case class Work(id: Long)

  case class Reply(id: Long)

  case class Done(id: Long)

  def props: Props =
    Props(new BalancerRouter).withDispatcher("akka.flow-dispatcher")
}

object ConsistentHashingRouter {

  case class CHWork(id: Long, key: String)

  case class DBObject2(id: Long, replyTo: ActorRef)

  def props: Props =
    Props(new ConsistentHashingRouter).withDispatcher("akka.flow-dispatcher")
}

//https://community.oracle.com/blogs/tomwhite/2007/11/27/consistent-hashing
class ConsistentHashingRouter extends ActorSubscriber with ActorLogging {
  var currentRouteeSize = 5
  var index             = 0
  var routeesMap        = Map.empty[Long, ActorRef]
  val keys              = Vector("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
  var removed           = false

  val routees = (0 until currentRouteeSize).map { i ⇒
    val name = "routee-" + i
    println("Create: " + name)
    context.actorOf(Props(new ChRoutee(name, i)).withDispatcher("akka.flow-dispatcher"), name)
  }

  val hashMapping: ConsistentHashMapping = {
    case CHWork(_, key) ⇒ key
  }

  //println("VirtualNodesFactor: " + context.system.settings.DefaultVirtualNodesFactor)
  /*val randevusLogic = new RoutingLogic {
    override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = ???
  }
   */

  val logic = akka.routing.ConsistentHashingRoutingLogic(context.system, currentRouteeSize, hashMapping)
  var router = akka.routing.Router(logic, routees.map { actor ⇒
    context.watch(actor)
    akka.routing.ActorRefRoutee(actor)
  })

  override protected def requestStrategy = new MaxInFlightRequestStrategy(32) {
    override def inFlightInternally = routeesMap.size
  }

  override def receive: Actor.Receive = {
    case Terminated(routee) ⇒
      router = router.removeRoutee(routee)
      if (router.routees.size == 0)
        (context stop self)

    case OnNext(DBObject2(id, requestor)) ⇒
      if (id % 32 == 0) {
        if (!removed) {
          router = router.removeRoutee(routees(index))
          println(s"Removed routee by index $index")
          index = index + 1
          removed = true
        } else {
          currentRouteeSize = currentRouteeSize + 1
          val name = "routee-" + currentRouteeSize
          println("Added routee " + name)
          router = router.addRoutee(
            context.actorOf(
              Props(new ChRoutee(name, currentRouteeSize))
                .withDispatcher("akka.flow-dispatcher"),
              name
            )
          )
          removed = false
        }
      }
      routeesMap += (id → requestor)
      router.route(CHWork(id, keys((id % keys.size).toInt)), self)
    case Reply(id) ⇒
      routeesMap(id) ! Done(id)
      routeesMap -= id

    case OnComplete ⇒
      log.info("worker-router has received OnComplete")
      routees.foreach { r ⇒
        (context stop r)
      }
  }
}

/**
  * Manually managed router
  */
class BalancerRouter extends ActorSubscriber with ActorLogging {

  //import BalancerRouter._

  val MaxInFlight = 32
  var requestors  = Map.empty[Long, ActorRef]
  val n           = Runtime.getRuntime.availableProcessors / 2

  val workers = (0 until n)
    .map(i ⇒ s"worker-$i")
    .map(name ⇒ context.actorOf(Props(classOf[Worker], name).withDispatcher("akka.flow-dispatcher"), name))

  var router = akka.routing.Router(akka.routing.RoundRobinRoutingLogic(), workers.map { r ⇒
    context.watch(r)
    akka.routing.ActorRefRoutee(r)
  })

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
        context.stop(self)
      }

    case OnNext(DBObject(id, requestor)) ⇒
      requestors += (id → requestor)
      router.route(Work(id), self)
    case Reply(id) ⇒
      requestors(id) ! Done(id)
      requestors -= id
    case OnComplete ⇒
      log.info("worker-router has received OnComplete")
      workers.foreach { r ⇒
        context.stop(r)
      }

    case OnError(ex) ⇒ log.info("OnError {}", ex.getMessage)
  }
}

class Worker(name: String) extends Actor with ActorLogging {
  override def receive = {
    case Work(id) ⇒
      Thread.sleep(java.util.concurrent.ThreadLocalRandom.current.nextInt(100, 150))
      //log.info("{} has done job {}", name, id)
      sender() ! Reply(id)
  }
}

class ChRoutee(name: String, workerId: Int) extends Actor with ActorLogging {
  override def receive = {
    case CHWork(id, key) ⇒
      Thread.sleep(100)
      //ThreadLocalRandom.current().nextInt(100, 150))
      log.info("Routee:{} gets messageId:{} key:{}", workerId, id, key)
      sender() ! Reply(id)
  }
}

class RecordsSink(name: String, val address: InetSocketAddress) extends Actor with ActorLogging with GraphiteMetrics {
  override def receive = {
    case BalancerRouter.Done(_) ⇒
      send(s"$name:1|c")
  }
}

/**
  * Same as throttledSource
  *
  * @param name
  * @param address
  * @param delay
  */
class TopicReader(name: String, val address: InetSocketAddress, delay: Long)
    extends ActorPublisher[Int]
    with GraphiteMetrics {
  val Limit      = 10000
  var progress   = 0
  val observeGap = 1000

  override def receive: Actor.Receive = {
    case Request(n) ⇒
      if (isActive && totalDemand > 0) {
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

class PubSubSink private (name: String, val address: InetSocketAddress, delay: Long)
    extends ActorSubscriber
    with ActorPublisher[Long]
    with GraphiteMetrics {
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

  private def reply =
    while ((isActive && totalDemand > 0) && !queue.isEmpty) {
      send(s"$name:1|c")
      onNext(queue.dequeue)
    }
}

object SyncActor {
  def props(name: String, address: InetSocketAddress, delay: Long) =
    Props(new SyncActor(name, address, delay))
      .withDispatcher("akka.flow-dispatcher")

  def props2(name: String, address: InetSocketAddress) =
    Props(new SyncActor(name, address)).withDispatcher("akka.flow-dispatcher")

  def props3(name: String, address: InetSocketAddress, delay: Long) =
    Props(new SyncActor(name, address, delay))

  def props4(name: String, address: InetSocketAddress, delay: Long, limit: Long) =
    Props(new SyncActor(name, address, delay, limit))
      .withDispatcher("akka.flow-dispatcher")
}

class SyncActor private (name: String, val address: InetSocketAddress, delay: Long, limit: Long)
    extends ActorSubscriber
    with GraphiteMetrics {
  var count                              = 0
  override protected val requestStrategy = OneByOneRequestStrategy

  private def this(name: String, statsD: InetSocketAddress, delay: Long) {
    this(name, statsD, delay, 0L)
  }

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0L)
  }

  override def receive: Receive = {
    case OnNext(msg: Double) ⇒
      Thread.sleep(delay)
      send(s"$name:1|c")

    case OnNext(msg: Int) ⇒
      //println(s"${Thread.currentThread().getName}:  $msg")
      Thread.sleep(delay)
      send(s"$name:1|c")

    case OnNext(msg: (Int, Int, Int)) ⇒
      send(s"$name:1|c")

    case OnNext(msg: String) ⇒
      println(msg)
      Thread.sleep(delay)
      count += 1
      if (count == limit) {
        println(s"Limit $limit has been achieved")
        context.system.stop(self)
      }

    case OnNext(log: LogEntry) ⇒
      send(s"$name:1|c")

    case OnNext(q: CircularFifo[Int]) ⇒
      send(s"$name:1|c")
      println(s"sliding window ${q}")

    case OnNext(s: SimpleMAState[_] @unchecked) ⇒
      send(s"$name:1|c")
      println(s"${s.ma}")

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
    Props(new BatchActor(name, address, delay, bufferSize))
      .withDispatcher("akka.flow-dispatcher")
}

class BatchActor private (name: String, val address: InetSocketAddress, delay: Long, bufferSize: Int)
    extends ActorSubscriber
    with GraphiteMetrics {
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

  private def flush() =
    while (!queue.isEmpty) {
      val _ = queue.dequeue
      send(s"$name:1|c")
    }
}

object DegradingActor {
  def props(name: String, address: InetSocketAddress, delayPerMsg: Long, initialDelay: Long) =
    Props(new DegradingActor(name, address, delayPerMsg, initialDelay))
      .withDispatcher("akka.flow-dispatcher")

  def props2(name: String, address: InetSocketAddress, delayPerMsg: Long) =
    Props(new DegradingActor(name, address, delayPerMsg))
      .withDispatcher("akka.flow-dispatcher")
}

class DegradingActor private (val name: String, val address: InetSocketAddress, delayPerMsg: Long, initialDelay: Long)
    extends ActorSubscriber
    with GraphiteMetrics {

  var delay       = 0L
  var lastSeenMsg = 0
  override protected val requestStrategy: RequestStrategy =
    OneByOneRequestStrategy

  private def this(name: String, statsD: InetSocketAddress) {
    this(name, statsD, 0, 0)
  }

  private def this(name: String, statsD: InetSocketAddress, delayPerMsg: Long) {
    this(name, statsD, delayPerMsg, 0)
  }

  override def receive: Receive = {
    case OnNext(msg: Int) ⇒
      //println(Thread.currentThread().getName + " degrading")
      delay += delayPerMsg
      val latency = initialDelay + (delay / 1000)
      Thread.sleep(latency, (delay % 1000).toInt)
      lastSeenMsg = msg
      send(s"$name:1|c")

    case OnNext(msg: Long) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: (Int, Int)) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: AkkaRecipes.State) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnNext(msg: (AkkaRecipes.State, Int)) ⇒
      println(msg)
      send(s"$name:1|c")

    case OnComplete ⇒
      println(s"Complete DegradingActor $lastSeenMsg")
      (context stop self)
  }
}

object DegradingActorSink {

  sealed trait DegradingActorProtocol

  case object Init extends DegradingActorProtocol

  case class StreamFailure(th: Throwable) extends DegradingActorProtocol

  case object Ack extends DegradingActorProtocol

  case object OnCompleted extends DegradingActorProtocol

  def props(name: String, address: InetSocketAddress, delayPerMsg: Long) =
    Props(new DegradingActorSink(name, address, delayPerMsg))
      .withDispatcher("akka.blocking-dispatcher")
}

object DegradingTypedActorSource {

  sealed trait Confirm
  case object Confirm                                                                            extends Confirm
  case class Connect(ref: akka.actor.typed.ActorRef[DegradingTypedActorSource.TypedSrcProtocol]) extends Confirm

  sealed trait TypedSrcProtocol
  case object Completed                   extends TypedSrcProtocol
  case class StreamFailure(ex: Throwable) extends TypedSrcProtocol
  case class IntValue(value: Int)         extends TypedSrcProtocol
}

object DegradingTypedActorSinkPb {

  import akka.actor.typed.{Behavior, PostStop, Terminated}
  import akka.actor.typed.scaladsl.Behaviors

  sealed trait Ack

  case object Ack extends Ack

  sealed trait AckProtocol

  case class Init(sender: akka.actor.typed.ActorRef[Ack]) extends AckProtocol

  case class Next(sender: akka.actor.typed.ActorRef[Ack], m: Int) extends AckProtocol

  case object Completed extends AckProtocol

  case class Failed(ex: Throwable) extends AckProtocol

  def apply(name: String, gr: GraphiteMetrics, delayPerMsg: Long, initialDelay: Long): Behavior[AckProtocol] =
    Behaviors.receive[AckProtocol] {
      case (ctx, _ @Init(sender)) ⇒
        ctx.log.info(s"Init $name !!!")
        sender ! Ack
        go(name, gr, 0L, delayPerMsg, initialDelay)
    }

  private def go(
    name: String,
    gr: GraphiteMetrics,
    delay: Long,
    delayPerMsg: Long,
    initialDelay: Long
  ): Behavior[AckProtocol] =
    Behaviors
      .receive[AckProtocol] {
        case (ctx, _ @Next(sender, v)) ⇒
          val d       = delay + delayPerMsg
          val latency = initialDelay + (d / 1000)
          Thread.sleep(latency, (d % 1000).toInt)

          if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.99)
            ctx.log.info(s"$name got:${v} latency:$latency ms")

          gr.send(s"$name:1|c")

          sender ! Ack
          go(name, gr, d, delayPerMsg, initialDelay)
        case (ctx, _ @Failed(ex)) ⇒
          ctx.log.error(ex, "Failed: ")
          Behavior.stopped
        case (ctx, Completed) ⇒
          ctx.log.error(s"Completed $name !!!")
          Behavior.stopped
      }
      .receiveSignal {
        case (ctx, PreRestart) ⇒
          ctx.log.error(s"PreRestart $name !!!!")
          Behaviors.same
        case (ctx, PostStop) ⇒
          ctx.log.error("PostStop !!!!")
          Behaviors.stopped
        case (ctx, Terminated(actor)) ⇒
          ctx.log.error(s"Terminated: $actor !!!!")
          Behaviors.stopped
        case (ctx, other) ⇒
          ctx.log.error(s"Other signal: $other  !!!")
          Behaviors.stopped
      }
}

object DegradingTypedActorSink {

  import akka.actor.typed.{Behavior, PostStop, Terminated}
  import akka.actor.typed.scaladsl.Behaviors

  sealed trait Protocol

  case object OnCompleted extends Protocol

  case class StreamFailure(ex: Throwable)             extends Protocol
  case class IntValue(value: Int)                     extends Protocol
  case class RecoverableSinkFailure(cause: Throwable) extends Exception(cause) with NoStackTrace

  def apply(name: String, gr: GraphiteMetrics, delayPerMsg: Long, initialDelay: Long): Behavior[Protocol] =
    Behaviors.setup[Protocol] { ctx ⇒
      ctx.log.info(s"Start $name !!!")
      go(name, gr, 0L, delayPerMsg, initialDelay)
    }

  private def go(
    name: String,
    gr: GraphiteMetrics,
    delay: Long,
    delayPerMsg: Long,
    initialDelay: Long
  ): Behavior[Protocol] =
    Behaviors
      .receive[Protocol] {
        case (ctx, elem: IntValue) ⇒
          val d       = delay + delayPerMsg
          val latency = initialDelay + (d / 1000)
          Thread.sleep(latency, (d % 1000).toInt)

          if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.99)
            ctx.log.info(s"$name got:${elem.value} latency:$latency ms")

          if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.999)
            throw RecoverableSinkFailure(new Exception(s"Recoverable error in $name !!!"))

          if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.99992)
            throw new Exception(s"Unrecoverable error in $name !!!")

          gr.send(s"$name:1|c")

          go(name, gr, d, delayPerMsg, initialDelay)
      }
      .receiveSignal {
        case (ctx, PreRestart) ⇒
          ctx.log.error(s"PreRestart $name !!!!")
          Behaviors.same
        case (ctx, PostStop) ⇒
          ctx.log.error("PostStop !!!!")
          Behaviors.stopped
        case (ctx, Terminated(actor)) ⇒
          ctx.log.error(s"Terminated: $actor !!!!")
          Behaviors.stopped
        case (ctx, other) ⇒
          ctx.log.error(s"Other signal: $other  !!!")
          Behaviors.stopped
      }
}

class DegradingActorSink private (
  val name: String,
  val address: InetSocketAddress,
  delayPerMsg: Long,
  initialDelay: Long
) extends Actor
    with GraphiteMetrics {

  def this(name: String, statsD: InetSocketAddress) =
    this(name, statsD, 0, 0)

  def this(name: String, statsD: InetSocketAddress, delayPerMsg: Long) =
    this(name, statsD, delayPerMsg, 0)

  override def preStart(): Unit =
    println(s"${Thread.currentThread.getName} $name started !!!")

  override def postStop(): Unit =
    println(s"${Thread.currentThread.getName} $name stopped !!!")

  def awaitInitialization: Receive = {
    case DegradingActorSink.Init ⇒
      sender() ! DegradingActorSink.Ack
      context.become(active(0L))
    case other ⇒
      throw new Exception("Unexpected msg" + other)
  }

  def active(delay: Long): Receive = {
    case msg: Int ⇒
      val d       = delay + delayPerMsg
      val latency = initialDelay + (d / 1000)
      Thread.sleep(latency, (d % 1000).toInt)

      if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.9995 /*&& !name.contains("7_1-3")*/ ) {
        println(s"${Thread.currentThread.getName} Boom $name !!!!!")
        //throw new Exception("Boom !!!")
        //sender() ! DegradingBlockingActor.StreamFailure(new Exception("Boom !!!"))
        context stop self
      }

      if (java.util.concurrent.ThreadLocalRandom.current.nextDouble > 0.99)
        println(s"${Thread.currentThread.getName} $name got:$msg Degrade:$latency")

      send(s"$name:1|c")
      sender() ! DegradingActorSink.Ack
      context.become(active(d))

    case DegradingActorSink.OnCompleted ⇒
      context stop self
    case DegradingActorSink.StreamFailure(ex) ⇒
      context stop self
      println("Stream has been completed: " + ex.getMessage)
    case other ⇒
      println("Unexpected msg: " + other)
      sender() ! DegradingActorSink.StreamFailure(new Exception("Unexpected message: " + other))
      context stop self
  }

  override def receive: Receive = awaitInitialization
}

class DbCursorPublisher(name: String, val Limit: Long, val address: InetSocketAddress)
    extends ActorPublisher[Long]
    with GraphiteMetrics
    with ActorLogging {
  var limit      = 0L
  var seqN       = 0L
  val showPeriod = 50

  override def receive: Receive = {
    case Request(n) if (isActive && totalDemand > 0) ⇒
      log.info("request: {}", n)
      if (seqN >= Limit)
        onCompleteThenStop()

      limit = n
      while (limit > 0) {
        Thread.sleep(200)
        seqN += 1
        limit -= 1

        if (limit % showPeriod == 0) {
          log.info("Cursor progress: {}", seqN)
          Thread.sleep(500)
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

  override def receive = run(0L)

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
        i += 1
        Item(i)
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

  def props: Props =
    Props[BatchProducer].withDispatcher("akka.flow-dispatcher")
}

object IndividualRateLimiter {

  case object RateLimitExceeded extends RuntimeException

}

class IndividualRateLimiter(number: Int, period: FiniteDuration) {

  import IndividualRateLimiter._

  //the index of the next slot to be used
  private var cursor = 0

  private val startTimes = Array.fill(number)(Deadline.now - period)

  private def enqueue(time: Deadline) = {
    startTimes(cursor) = time
    cursor += 1
    if (cursor == number) cursor = 0
  }

  def require[T](block: ⇒ Future[T]): Future[T] = {
    val now = Deadline.now
    if ((now - startTimes(cursor)) < period) Future.failed(RateLimitExceeded)
    else {
      enqueue(now)
      block
    }
  }
}

//Custom linear processing stages using GraphStage
//http://rnduja.github.io/2016/03/25/a_first_look_to_akka_stream/
//This will become useful if you want to replay events from log (let's say) that have ts attached to each line.
class TimeStampedLogReader[T](time: T ⇒ Long) extends GraphStage[FlowShape[T, T]] {
  var firstEventTime  = 0L
  var firstActualTime = 0L

  val in  = Inlet[T]("RateAdaptor.in")
  val out = Outlet[T]("RateAdaptor.out")

  override def shape: FlowShape[T, T] = FlowShape.of(in, out)

  //should be executed in separate dispatched due to Thread.sleep
  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem        = grab(in)
            val actualTime  = System.currentTimeMillis
            val eventTime   = time(elem)
            val actualDelay = actualTime - firstActualTime
            val eventDelay  = eventTime - firstEventTime

            if (firstActualTime != 0L) {
              if (actualDelay < eventDelay) {
                val iterationLatency = eventDelay - actualDelay
                println(s"${Thread.currentThread.getName}: sleep $iterationLatency")
                Thread sleep iterationLatency
              }
            } else {
              firstActualTime = actualTime
              firstEventTime = eventTime
            }
            push(out, elem)
          }
        }
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
}

/**
  *
  * val resultFuture = Source(1 to 5)
  * .via(new Filter(_ % 2 == 0))
  * .via(new Duplicator())
  * .runWith(sink)
  */
class Filter[A](p: A ⇒ Boolean) extends GraphStage[FlowShape[A, A]] {
  val in  = Inlet[A]("Filter.in")
  val out = Outlet[A]("Filter.out")

  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new GraphStageLogic(shape) {
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          if (p(elem)) push(out, elem) else pull(in)
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          pull(in)
      })
    }
}

//Reactive_streams_principles_applied_in_akka_streams_by Eric Loots
object DelayFlow {

  type Element = (Double, Double)

  /**
    * Allows you to delay elements.
    * Example:
    * given `delay` = 5 and `scaleFactor` = 0.5 and the input
    * `(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),  (7,0),  (8,0) ...` results in the pairs
    * `(0,1),(0,2),...              ,(1,3.5),(2,4.5),(3, 5.5) ... `
    *
    */
  def apply(delay: Int, scaleFactor: Double): Flow[Element, Element, NotUsed] =
    Flow[Element]
      .statefulMapConcat { () ⇒
        // mutable state needs to be kept inside the stage
        var index                     = 0
        val ringBuffer: Array[Double] = Array.ofDim[Double](delay)

        {
          case (sample, sample1) ⇒
            val prev = ringBuffer(index)
            ringBuffer(index) = sample
            index = (index + 1) % delay
            scala.collection.immutable.Iterable((prev, sample1 + prev * scaleFactor))
        }
      }
}

object MovingAvg {

  def apply[T: SourceElement: Numeric: ClassTag](capacity: Int): Flow[T, Double, NotUsed] =
    Flow[T].statefulMapConcat[Double] { () ⇒
      var ma: Option[Double]  = None
      val d: SourceElement[T] = implicitly[SourceElement[T]]

      var index        = 0
      val rb: Array[T] = Array.ofDim[T](capacity)

      //HyperLogLog

      { element ⇒
        val prev = rb(index)
        rb(index) = element
        index = (index + 1) % capacity

        if (ma.isDefined)
          ma = ma.map(_ + ((d(element) - d(prev)) / capacity))

        if (index + 1 == capacity && ma.isEmpty) //one shot action
          ma = Some(d(rb.sum) / capacity)

        ma.fold(scala.collection.immutable.Iterable(0.0)) {
          scala.collection.immutable.Iterable(_)
        }
      }
    }
}

/**
  * Another way to implement TrailingDifference using `statefulMapConcat`.
  * As an alternative to http://blog.lancearlaus.com/akka/streams/scala/2015/05/27/Akka-Streams-Balancing-Buffer/
  *
  * Example:
  * given `delay` = 5 the input
  * `0, 1, 2, 3, 4, 5   , 6   , 7   , 8     ...` results in the pairs
  * `0,...        ,(5-0),(6-1),(7-2),(8-3) ... `
  *
  */
object TrailingDifference {

  def apply[T: Numeric: ClassTag](capacity: Int): Flow[T, T, NotUsed] =
    Flow[T].statefulMapConcat[T] { () ⇒
      var seqNum: Long            = 0L //(Int.MaxValue - 2).toLong
      val slidingWindow: Array[T] = Array.ofDim[T](capacity)

      { element ⇒
        val i    = (seqNum % capacity).toInt
        val prev = slidingWindow(i)
        slidingWindow(i) = element

        val it = if (seqNum >= capacity) {
          //println(s"$seqNum - $i : $element - $prev")
          scala.collection.immutable.Iterable(implicitly[Numeric[T]].minus(element, prev))
        } else
          scala.collection.immutable.Iterable(implicitly[Numeric[T]].zero)

        seqNum += 1L

        it
      }
    }
}

/*
object Deduplicator {

  case class EventEnvelope(sequenceNr: Long)
  case class Deduplicated(event: EventEnvelope, isDuplicate: Boolean)

  //https://pavkin.ru/multiple-faces-of-scala-iterator-trap/
  // Provides event deduplication, multiplexed over several "consumers".
  // Each consumer is tagged with a string key, which is used in the versions Map
  def apply(initialVersions: Map[String, Long]): Flow[EventEnvelope, Map[String, Deduplicated], NotUsed] =
    Flow[EventEnvelope]
      .statefulMapConcat(
        () ⇒ {
          //safe to keep state here
          var lastVersions = initialVersions

          //invoked on  every element
          {
            e: EventEnvelope ⇒
              // for each consumer key deduplicate the event and provide new threshold version

              val deduplicated = lastVersions.view.mapValues { lastVersion ⇒
                val isOriginal = e.sequenceNr > lastVersion
                if (isOriginal) e.sequenceNr → Deduplicated(e, false)
                else lastVersion             → Deduplicated(e, true)
              }

              lastVersions = deduplicated.mapValues(_._1)
              // pass deduplicated events further down the stream
              //return a List that shall be emitted
              scala.collection.immutable.Iterable(deduplicated.view.mapValues(_._2))
          }
        }
      )
}
 */

class Duplicator[A] extends GraphStage[FlowShape[A, A]] {
  val in  = Inlet[A]("Duplicator.in")
  val out = Outlet[A]("Duplicator.out")

  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      //all mutable state MUST be inside the GraphStageLogic
      var lastElem: Option[A] = None

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            lastElem = Option(elem)
            push(out, elem)
          }

          override def onUpstreamFinish(): Unit = {
            if (lastElem.isDefined) emit(out, lastElem.get)
            complete(out)
          }
        }
      )

      setHandler(out, new OutHandler {
        override def onPull(): Unit =
          if (lastElem.isDefined) {
            push(out, lastElem.get)
            lastElem = None
          } else {
            pull(in)
          }
      })
    }
}

//Same as Duplicator but use emitMultiple to avoid mutable state
class DuplicatorN[A] extends GraphStage[FlowShape[A, A]] {
  val in  = Inlet[A]("Duplicator.in")
  val out = Outlet[A]("Duplicator.out")

  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          // this will temporarily suspend this handler until the two elems
          // are emitted and then reinstates it
          emitMultiple(out, immutable.Iterable(elem, elem))
        }
      }
    )

    setHandler(out, new OutHandler {
      override def onPull() = pull(in)
    })
  }
}

class TimedGate[A](silencePeriod: FiniteDuration) extends GraphStage[FlowShape[A, A]] {
  val in    = Inlet[A]("TimedGate.in")
  val out   = Outlet[A]("TimedGate.out")
  val shape = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes) =
    new TimerGraphStageLogic(shape) {
      var open = false
      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          val elem = grab(in)
          if (open) pull(in)
          else {
            push(out, elem)
            open = true
            scheduleOnce(None, silencePeriod)
          }
        }
      })
      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })

      override protected def onTimer(timerKey: Any): Unit =
        open = false
    }
}

//http://www.cakesolutions.net/teamblogs/lifting-machine-learning-into-akka-streams
//GraphStage[FlowShape[A, immutable.Seq[A]]]
/*

class SlidingWindow[A](size: Int) extends PushPullStage[A, List[A]] {

  val in = Inlet[A]("in")
  val out = Outlet[immutable.Seq[A]]("out")

  require(size > 0)

  private val buffer = mutable.Queue[A]()
  private var isSaturated = false

  def onPush(elem: A, ctx: Context[List[A]]) = {
    if (buffer.length == size) {
      // Buffer is full, so push new window
      buffer.dequeue //drop element ????
      buffer.enqueue(elem)
      ctx.push(buffer.toList)
    } else {
      // Buffer is not yet full, so keep consuming from our upstream
      buffer.enqueue(elem)
      if (buffer.length == size) {
        // Buffer has become full, so push new window and record saturation
        isSaturated = true
        ctx.push(buffer.toList)
      } else ctx.pull()
    }
  }

  override def onPull(ctx: Context[List[A]]) = {
    if (ctx.isFinishing) {
      // Streaming stage is shutting down, so we ensure that all buffer elements are flushed prior to finishing
      if (buffer.isEmpty) {
        // Buffer is empty, so we simply finish
        ctx.finish()
      } else if (buffer.length == 1) {
        // Buffer is non-empty, so empty it by sending undersized (non-empty) truncated window sequence and finish
        if (isSaturated) {
          // Buffer was previously saturated, so head element has already been seen
          buffer.dequeue()
          ctx.finish()
        } else {
          // Buffer was never saturated, so head element needs to be pushed
          ctx.pushAndFinish(buffer.dequeue :: Nil)
        }
      } else {
        // Buffer is non-empty, so empty it by sending undersized (non-empty) truncated window sequence - we will eventually finish here
        if (isSaturated) {
          // Buffer was previously saturated, so head element has already been seen
          buffer.dequeue()
          ctx.push(buffer.toList)
        } else {
          // Buffer was never saturated, so head element should be part of truncated window
          val window = buffer.toList
          buffer.dequeue()
          ctx.push(window)
        }
      }
    } else ctx.pull()
  }

  override def onUpstreamFinish(ctx: Context[List[A]]): TerminationDirective = {
    ctx.absorbTermination()
  }
}
 */

/*
class SlidingWindowTest extends AkkaSpec {
  import StreamTestKit._
  val in = PublisherProbe[String]()
  val out = SubscriberProbe[List[String]]()
  // Handle requests automatically and publish messages when available
  val pub = new AutoPublisher(in)

  Flow[String].transform(() => SlidingWindow[String](windowSize))
    .runWith(Source(in), Sink(out))

  val sub = out.expectSubscription()
  sub.request(msgs.length)
  for (msg <- msgs) {
    pub.sendNext(msg)
  }
}
 */

/*
trait GraphiteMetrics {
  val Encoding = "utf-8"
  val sendBuffer = (ByteBuffer allocate 512)
  val channel = DatagramChannel.open()

  def address: InetSocketAddress

  def send(message: String) = {
    sendBuffer.put(message.getBytes(Encoding))
    sendBuffer.flip()
    channel.send(sendBuffer, address)
    sendBuffer.limit(sendBuffer.capacity())
    sendBuffer.rewind()
  }
}*/

object Traverse {

  /**
    * Applies a future-returning function to each element in a collection, and
    * return a Future of a collection of the results (as in `Future.traverse`),
    * but with bounded maximal parallelism.
    * Uses Akka Streams' `mapAsync` to achieve maximum throughput, rather than processing in fixed batches.
    *
    * @param in          collection of operands.
    * @param maxParallel the maximum number of threads to use.
    * @param f           an asynchronous operation.
    * @return Future of the collection of results.
    */
  def traverse[A, B](in: TraversableOnce[A], maxParallel: Int)(
    f: A ⇒ Future[B]
  )(implicit mat: ActorMaterializer): Future[Seq[B]] =
    Source[A](in.toStream)
      .mapAsync(maxParallel)(f)
      .toMat(Sink.seq)(Keep.right)
      .run()

  /** Future.sequence, but with bounded parallelism */
  def sequence[A](in: TraversableOnce[Future[A]], maxParallel: Int)(
    implicit
    mat: ActorMaterializer
  ): Future[Seq[A]] =
    traverse(in, maxParallel)(identity)
}
