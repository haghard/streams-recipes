package recipes

import java.util.concurrent.Executor

import java.lang.{Long ⇒ JLong}

import akka.event.LoggingAdapter
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.scaladsl.{Keep, Source}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler, StageLogging}
import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement, ResultSet, Row}
import com.google.common.util.concurrent.ListenableFuture
import recipes.CustomStages.LastSeen

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import PsJournal._

/*
http://doc.akka.io/docs/akka/2.5.4/scala/stream/stream-customize.html
Thread safety of custom processing stages.
  The callbacks are never called concurrently.
  The state encapsulated can be safely modified from the provided callbacks, without any further synchronization.
 */

/**
  * A Source that has one output and no inputs, it models a source of cassandra rows
  * associated with a persistenceId starting with offset.
  *
  * The impl is based on this
  * https://github.com/akka/alpakka/blob/master/cassandra/src/main/scala/akka/stream/alpakka/cassandra/impl/CassandraSourceStage.scala
  * and adapted with respect to akka-cassandra-persistence schema
  */
final class PsJournal(
  client: Cluster,
  keySpace: String,
  journal: String,
  persistenceId: String,
  offset: Long,
  partitionSize: Long,
  pageSize: Int
) extends GraphStage[SourceShape[Row]] {
  val out: Outlet[Row] = Outlet[Row](akka.event.Logging.simpleName(this) + ".out")

  private val retryTimeout = 3000

  override val shape: SourceShape[Row] = SourceShape(out)

  private val queryByPersistenceId =
    s"""
       |SELECT persistence_id, partition_nr, sequence_nr, timestamp, timebucket, event FROM $journal WHERE
       |  persistence_id = ? AND
       |  partition_nr = ? AND
       |  sequence_nr >= ?
       """.stripMargin

  /*
    Selecting a separate dispatcher in Akka Streams is done by returning it from the initialAttributes of the GraphStage.
   */
  override protected def initialAttributes: Attributes =
    Attributes.name(persistenceId)
  //.and(ActorAttributes.dispatcher("cassandra-dispatcher"))

  private def navigatePartition(sequenceNr: Long, partitionSize: Long): Long = sequenceNr / partitionSize

  private def statement(
    preparedStmt: PreparedStatement,
    persistenceId: String,
    partition: JLong,
    sequenceNr: JLong,
    pageSize: Int
  ) =
    new BoundStatement(preparedStmt).bind(persistenceId, partition, sequenceNr).setFetchSize(pageSize)

  @tailrec private def conAttempt[T](n: Int)(log: LoggingAdapter, f: ⇒ T): T = {
    log.info("Getting cassandra connection")
    Try(f) match {
      case Success(x) ⇒
        x
      case Failure(e) if n > 1 ⇒
        log.error(e.getMessage)
        Thread.sleep(retryTimeout)
        conAttempt(n - 1)(log, f)
      case Failure(e) ⇒
        throw e
    }
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {
      /*
        It is not safe to access the state of any custom stage outside of the callbacks that it provides,
        just like it is unsafe to access the state of an actor from the outside.
        This means that Future callbacks should not close over internal state of custom stages because such access can be
        concurrent with the provided callbacks, leading to undefined behavior.

        All mutable state MUST be inside the GraphStageLogic
       */
      var requireMore                                      = false
      var sequenceNr                                       = offset
      var partitionIter                                    = Option.empty[ResultSet]
      var onMessageCallback: AsyncCallback[Try[ResultSet]] = _

      //
      lazy val session      = conAttempt(Int.MaxValue)(log, client.connect(keySpace))
      lazy val preparedStmt = session.prepare(queryByPersistenceId)
      implicit lazy val ec  = materializer.executionContext

      override def preStart(): Unit = {
        onMessageCallback = getAsyncCallback[Try[ResultSet]](onFetchCompleted)
        val partition = navigatePartition(sequenceNr, partitionSize): JLong
        val stmt      = statement(preparedStmt, persistenceId, partition, sequenceNr, pageSize)
        session.executeAsync(stmt).asScala.onComplete(onMessageCallback.invoke)
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            partitionIter match {
              case Some(iter) if iter.getAvailableWithoutFetching > 0 ⇒
                sequenceNr += 1
                push(out, iter.one)
              case Some(iter) ⇒
                if (iter.isExhausted) {
                  //Reached the end of the current partition. Try to read from the next partition.
                  val nextPartition = navigatePartition(sequenceNr, partitionSize): JLong
                  val stmt          = statement(preparedStmt, persistenceId, nextPartition, sequenceNr, pageSize)
                  session.executeAsync(stmt).asScala.onComplete(onMessageCallback.invoke)
                } else {
                  //Your page size less than akka-cassandra-persistence partition size(cassandra-journal.target-partition-size)
                  //so you hit the end of page but still have something to read
                  log.info("Still have something to read in current partition seqNum: {}", sequenceNr)
                  iter.fetchMoreResults.asScala.onComplete(onMessageCallback.invoke)
                }
              case None ⇒
                //log.info("A request from a downstream had arrived before we read the first row")
                ()
            }
        }
      )

      /*
       * Reached either
       * the end of the page because of fetchSize  or
       * the end of journal corresponding to given persistence id
       */
      private def onFetchCompleted(rsOrFailure: Try[ResultSet]): Unit =
        rsOrFailure match {
          case Success(iter) ⇒
            partitionIter = Some(iter)
            if (iter.getAvailableWithoutFetching > 0) {
              if (isAvailable(out)) {
                sequenceNr += 1
                push(out, iter.one)
              }
            } else {
              log.info("{} CompleteSource {} seqNum:{}", persistenceId, sequenceNr)
              completeStage()
            }

          case Failure(failure) ⇒ failStage(failure)
        }

      override def postStop: Unit = {
        //cleaning up resources should be done here
        //session.closeAsync
      }
    }
}

object PsJournal {

  implicit class ListenableFutureConverter[A](val lf: ListenableFuture[A]) extends AnyVal {
    def asScala(implicit ec: ExecutionContext): Future[A] = {
      val promise = Promise[A]
      lf.addListener(() ⇒ promise.complete(Try(lf.get)), ec.asInstanceOf[Executor])
      promise.future
    }
  }

  def apply[T: ClassTag](
    client: Cluster,
    keySpace: String,
    journal: String,
    persistenceId: String,
    offset: Long,
    partitionSize: Long,
    pageSize: Int = 32
  ) =
    Source
      .fromGraph(new PsJournal(client, keySpace, journal, persistenceId, offset, partitionSize, pageSize))
      .map(???)
      .viaMat(new LastSeen)(Keep.right)
}
