package ddd

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

package object impl {

  case class DddDaemons(name: String) extends ThreadFactory {
    private def namePrefix         = s"$name-thread"
    private val threadNumber       = new AtomicInteger(1)
    private val group: ThreadGroup = Thread.currentThread().getThreadGroup
    override def newThread(r: Runnable) = {
      val t = new Thread(group, r, s"$namePrefix-${threadNumber.getAndIncrement()}", 0L)
      t.setDaemon(true)
      t
    }
  }
}
