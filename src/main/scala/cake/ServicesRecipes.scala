package cake

import java.util.concurrent.TimeUnit

//runMain cake.ServicesRecipes
object ServicesRecipes extends App {

  val c = new java.util.concurrent.CountDownLatch(3)

  cake.ShapelessProgram.gather
    .unsafePerformAsync { r ⇒
      println(r)
      c.countDown()
    }

  cake.ShapelessProgram.gatherZip
    .unsafePerformAsync { r ⇒
      println(r)
      c.countDown()
    }

  cake.ProgramWithTask.gatherS5
    .unsafePerformAsync { r ⇒
      println(r)
      c.countDown()
    }

  c.await(3, TimeUnit.SECONDS)
}