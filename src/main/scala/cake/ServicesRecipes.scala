package cake

import java.util.concurrent.TimeUnit

//runMain cake.ServicesRecipes
object ServicesRecipes extends App {

  val c = new java.util.concurrent.CountDownLatch(3)

  cake.ProgramWithTask0.gather
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  cake.ProgramWithTask0.gatherZip
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  cake.ProgramWithTask.gatherS5
    .runAsync { r ⇒
      println(r)
      c.countDown()
    }

  c.await(3, TimeUnit.SECONDS)
}