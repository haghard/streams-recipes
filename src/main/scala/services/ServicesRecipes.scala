package services

import java.util.concurrent.TimeUnit

//runMain services.ServicesRecipes
object ServicesRecipes extends App {

  val c = new java.util.concurrent.CountDownLatch(1)

  services.ApplicationTaskService.gatherZip
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  c.await(3, TimeUnit.SECONDS)
}