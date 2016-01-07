package cake

import java.util.concurrent.TimeUnit

//runMain cake.ServicesRecipes
object ServicesRecipes extends App {

  val c = new java.util.concurrent.CountDownLatch(1)

  cake.ApplicationTaskService.gatherZip
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  c.await(3, TimeUnit.SECONDS)
}