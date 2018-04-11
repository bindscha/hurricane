package ch.epfl.labos.hurricane

import ch.epfl.labos.hurricane.backend.HurricaneBackend
import ch.epfl.labos.hurricane.frontend.HurricaneFrontend

object HurricaneApp {

  def main(args: Array[String]): Unit = {
    Config.ModeConfig.mode match {
      case Config.ModeConfig.Dev =>
        // Testing: starting 3 backend nodes and 1 frontend node
        HurricaneBackend.main(Seq("--me", "0").toArray)
        HurricaneBackend.main(Seq("--me", "1").toArray)
        HurricaneBackend.main(Seq("--me", "2").toArray)
        HurricaneFrontend.main(args)
        // XXX: cannot start more frontends because Config is a shared singleton object, which will cause quite a few issues (e.g., finding out Config.HurricaneConfig.me)
      case Config.ModeConfig.Prod =>
        // Prod: starting 1 backend node and 1 frontend node
        HurricaneBackend.main(Array.empty)
        Thread.sleep(5000) // sleep 2 seconds to make sure the backend is started
        HurricaneFrontend.main(args)
    }
  }

}