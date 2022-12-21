package Philosophers

import java.util.concurrent.Semaphore

object Main_philosophers {
  def main(args: Array[String]): Unit = {
    val hostPort = "localhost:2181"
    val philosophersCount = 5
    val waiting = philosophersCount

    val forks = new Array[Semaphore](philosophersCount)
    for (j <- 0 until philosophersCount){
      forks(j) = new Semaphore(1)
    }

    val threads = new Array[Thread](philosophersCount)
    for (id <- 0 until philosophersCount){
      threads(id) = new Thread(
        new Runnable {
          def run(): Unit = {
            val i = (id + 1) % philosophersCount
            val philosopher = Philosopher(id, hostPort, "/ph".toString, forks(id), forks(i), waiting)
            for (j <- 1 to 2) {
              philosopher.eat()
              philosopher.think()
            }
          }
        }
      )
      threads(id).setDaemon(false)
      threads(id).start()
    }
    for (id <- 0 until philosophersCount){
      threads(id).join()
    }
  }
}

