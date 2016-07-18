package fs

import fs2.Stream._
import fs2._
import spinoco.fs2.zk.{client, _}

import scala.concurrent.duration._
import scala.util.{Left, Right}

/**
  Gaol You have a number of processes in your application and you want to one of them arises as a leader and perform some master's tasks on behave of the cluster

  Use cases:
   1) Hadoop uses zk for master election for the Name-Node of HDFS.
   2) Kafka cluster
   Each of broker's partition has its own leader, you don't have to have one single leader for the whole cluster
   Each leader is a leader of own partition

    Zookeeper gives you a consistent view of nodes
    Znodes live in a hierarchy
    Znodes can be created|deleted|update|check on existence
    You can implement locks, barriers, master election

  */
object ServiceRegistry extends App {
  val address = "192.168.0.182:2181"

  val serviceDir = ZkNode.parse("/fs").get
  val nodeA = (serviceDir / """http:192.168.0.1:9001\api\results""").get
  val nodeB = (serviceDir / """http:192.168.0.2:9001\api\results""").get
  val nodeC = (serviceDir / """http:192.168.0.3:9001\api\results""").get

  val ct = 5.seconds
  val nodes = Vector(serviceDir, nodeA, nodeB, nodeC)

  implicit val S: Strategy = Strategy.fromFixedDaemonPool(4, "zk")
  implicit val Sch: Scheduler = Scheduler.fromFixedDaemonPool(2, "zk-scheduler")

  def delay = time.sleep[Task](2.second)

  def showState[A]: fs2.Pipe[Task, A, Unit] = _.evalMap { nodes ⇒ Task.delay(println("< " + nodes)) }

  def zkClient(address: String): Stream[Task, ZkClient[Task]] =
    client[Task](ensemble = address, timeout = ct) flatMap {
      _.fold(state => Stream.fail(new Throwable(s"Failed to connect to server: $state")), { zkC => emit(zkC)})
    }

  val watcher = zkClient(address).flatMap { client ⇒
    client.exists(serviceDir).flatMap { stats =>
      stats.fold(client.childrenOf(serviceDir)) { r =>
        eval_(client.delete(serviceDir, None)) ++ client.childrenOf(serviceDir)
      }
    }
  }

  val writer =
    delay ++
      zkClient(address).flatMap { zkc =>
        eval_(zkc.create(serviceDir, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
          delay ++ eval_(zkc.create(nodeA, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
          delay ++ eval_(zkc.create(nodeB, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
          delay ++ eval_(zkc.create(nodeC, ZkCreateMode.Persistent, None, List(ZkACL.ACL_OPEN_UNSAFE))) ++
          delay ++ eval_(zkc.delete(nodeB, None)) ++
          delay ++ eval_(zkc.delete(nodeC, None)) ++
          delay ++ eval_(zkc.delete(nodeA, None)) ++
          delay ++ eval_(zkc.delete(serviceDir, None))
      }

  (watcher mergeDrainR writer)
    .map(_.map(_._1))
    .through(showState)
    .take(nodes.size * 2)
    .onError { ex: Throwable ⇒
      ex.printStackTrace()
      Stream.eval(Task.delay(println(s"Error: ${ex.getMessage}")))
    }
    .runLog
    .unsafeTimed(20.seconds)
    .unsafeRun
}