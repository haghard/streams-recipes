package recipes

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.{ActorAttributes, Attributes, FlowShape, Graph, KillSwitches, Outlet, OverflowStrategy, SourceShape}
import akka.stream.scaladsl.{BroadcastHub, Flow, FlowWithContext, GraphDSL, Keep, MergeHub, Sink, SinkQueueWithCancel, Source, StreamRefs}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.typed.scaladsl.ActorMaterializer
//import io.moia.streamee.Respondee
import org.reactivestreams.Publisher

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
  * https://github.com/moia-dev/streamee
  * https://github.com/hseeberger/xtream.git
  */
object StreameeLikeExamples {
  //import io.moia.streamee._

  val buffersSize  = 1 << 3
  implicit val mat = ActorMaterializer()(??? /*context.system*/ )
  implicit val ec  = mat.executionContext

  def authProcess: FlowWithContext[HttpRequest, ActorRef[HttpResponse], HttpResponse, ActorRef[HttpResponse], Any] =
    FlowWithContext[HttpRequest, ActorRef[HttpResponse]] //Promise[HttpResponse]
      .withAttributes(Attributes.inputBuffer(buffersSize, buffersSize))
      //.map { req: HttpRequest ⇒ null.asInstanceOf[HttpResponse] }
      .mapAsync(2) { req: HttpRequest ⇒
        Future(null.asInstanceOf[HttpResponse])(ExecutionContext.global)
      }

  def auth: FlowWithContext[HttpRequest, Promise[HttpResponse], HttpResponse, Promise[HttpResponse], Any] =
    FlowWithContext[HttpRequest, Promise[HttpResponse]]
      .withAttributes(Attributes.inputBuffer(buffersSize, buffersSize))
      .mapAsync(4) { req: HttpRequest ⇒
        Future(null.asInstanceOf[HttpResponse])(ec)
      }

  //case GetSinkRef(replyTo) =>
  StreamRefs
    .sinkRef()
    .to(sink)
    .run()
    .onComplete {
      case Success(sinkRef) ⇒ //replyTo ! sinkRef
      case Failure(cause)   ⇒ //logger.error("Cannot create GetTripExecutionSinkRef!", cause)
    }

  //
  val (sink, switch, done) =
    MergeHub
      .source[(HttpRequest, Promise[HttpResponse])](buffersSize)
      .viaMat(KillSwitches.single)(Keep.both)
      .via(auth)
      //.to(Sink.foreach { case (response, promise) ⇒ promise.complete(Success(response)) })
      .toMat(Sink.foreach { case (response, promise) ⇒ promise.complete(Success(response)) }) {
        case ((sink, switch), done) ⇒ (sink, switch, done)
      }
      .run()

  Source.queue(1, ???).to(sink).run()

  Source.queue(1, ???).to(sink).run()
  //akka.NotUsed
  //ProcessSink[HttpResponse, Any]
  /*val sink: Sink[(HttpResponse, Respondee[HttpResponse]), Any] =
    Sink.foreach { case (response, respondee) ⇒ respondee.tell(io.moia.streamee.Respondee.Response(response)) }
   */

  //auth.into(sink, 3.seconds, 4)

  /*
  def tapErrors[In, CtxIn, Out, CtxOut, Mat, E](
    f: Sink[(E, CtxOut), Any] ⇒ FlowWithContext[In, CtxIn, Out, CtxOut, Mat]
  ): FlowWithContext[In, CtxIn, Either[E, Out], CtxOut, Future[Mat]] = {
    val flow =
      Flow.fromMaterializer {
        case (mat, attr) ⇒
          //val bufSize = attr.get[akka.stream.Attributes.InputBuffer].get
          /*
            A MergeHub is a special streaming hub that is able to collect streamed elements from a dynamic set of producers.
            It consists of two parts, a Source and a Sink. The Source streams the element to a consumer from its merged inputs.
            Once the consumer has been materialized, the Source returns a materialized value which
            is the corresponding Sink. This Sink can then be materialized arbitrary many times, where each of the new materializations will feed its consumed elements to the
            original Source.
   */

          val ((sink, killSwitch), src) =
            MergeHub
              .source[(E, CtxOut)](perProducerBufferSize = 1 << 4)
              .viaMat(KillSwitches.single)(Keep.both)
              //.toMat(Sink.foreach { case (e, ctxOut) ⇒ (Left(e), ctxOut) })(Keep.both)
              .toMat(Sink.asPublisher(false))(Keep.both)
              //.toMat(Sink.queue[(E, CtxOut)](4))(Keep.both)
              //.addAttributes(Attributes.inputBuffer(bufSize.initial, bufSize.max)))(Keep.both)
              //.toMat(BroadcastHub.sink[(E, CtxOut)])(Keep.both)
              .run()(mat)

          /*def readLoop(srcQ: SinkQueueWithCancel[(E, CtxOut)]): Future[(Either[E, Out], CtxOut)] = {
            src.queue.pull()
            ???
          }*/

          def out(
            queue: SinkQueueWithCancel[(E, CtxOut)]
          )(implicit ec: ExecutionContext): Source[(Either[E, Out], CtxOut), akka.NotUsed] =
            Source.unfoldAsync[SinkQueueWithCancel[(E, CtxOut)], (Either[E, Out], CtxOut)](queue) {
              q: SinkQueueWithCancel[(E, CtxOut)] ⇒
                q.pull()
                  .map(_.map(el ⇒ (q, (Left(el._1), el._2))))
            }

          f(sink)
            .map(Right.apply)
            .asFlow
            .alsoTo(
              Flow[Any]
                .to(Sink.onComplete {
                  case Success(_)     ⇒ killSwitch.shutdown()
                  case Failure(cause) ⇒ killSwitch.abort(cause)
                })
            )
            .merge(Source.fromPublisher(src).map { case (e, ctxOut) ⇒ (Left(e), ctxOut) })
        //.merge(out(src)(mat.executionContext))
        //.merge(Source.futureSource[Either[E, Out], CtxOut](futureSrc))
        //.merge(Source.future[(Either[E, Out], CtxOut)](readLoop(src)))
        //.merge(src.map { case (e, ctxOut) => (Left(e), ctxOut) })
      }
    FlowWithContext.fromTuples(flow)
  }
   */

  /*
  def flow = akka.stream.scaladsl.Flow.fromMaterializer { (mat, _) ⇒
    val bufferSize = 1 << 4
    //many-to-one
    val ((sink, switch), source) =
      MergeHub
        .source[IN](1)
        .viaMat(KillSwitches.single)(Keep.both)
        //.toMat(BroadcastHub.sink[IN])(Keep.both)
        .toMat(Sink.queue[IN].addAttributes(Attributes.inputBuffer(bufferSize, bufferSize)))(Keep.both)
        .run()(mat)

    process.asFlow.alsoTo(
      Flow[Any].to(Sink.onComplete {
        case Success(_)     ⇒ switch.shutdown()
        case Failure(cause) ⇒ switch.abort(cause)
      })
    )

  }
  FlowWithContext.fromTuples(flow)
   */
}
