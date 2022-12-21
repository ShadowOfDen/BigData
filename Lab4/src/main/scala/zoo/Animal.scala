package zoo

import org.apache.zookeeper._

//создание класса Animal
case class Animal( name: String, hostPort: String, root: String, partySize: Integer)
  extends Watcher
{
  val zk = new ZooKeeper(hostPort, 1000, this)
  val mutex = new Object()
  val animalPath: String = root + "/" + name

  if (zk == null) throw new Exception("ZK is NULL.")

  // Реакция на события от Zookeeper
  override def process(event: WatchedEvent): Unit = {
    // синхронизация методов
    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
      mutex.notify()
    }
  }

  // Реализация метода enter
  def enter(): Boolean = {
    // Создание эфимерного узла
    zk.create(animalPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    // Блок синхронизации
    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for the others.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
    }
    false
  }

  def leave(): Unit = {
    zk.delete(animalPath, -1)
  }
}