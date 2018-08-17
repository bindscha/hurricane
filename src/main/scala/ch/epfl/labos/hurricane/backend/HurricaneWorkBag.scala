package ch.epfl.labos.hurricane.backend

import akka.actor._
import better.files.File
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.common._


object HurricaneWorkBag {
  def props(bag: Bag, root: File): Props =
    Props(classOf[HurricaneWorkBag], bag.id, root)
}

class HurricaneWorkBag(bag: Bag, root: File = File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  def receive = {
    case _ =>
  }

}
