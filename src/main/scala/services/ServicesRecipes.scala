package services

import java.util.concurrent.TimeUnit

//runMain services.ServicesRecipes
object ServicesRecipes extends App {

  val c = new java.util.concurrent.CountDownLatch(1)

  services.ApplicationTaskService.gatherPzip
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  c.await(5, TimeUnit.SECONDS)
}