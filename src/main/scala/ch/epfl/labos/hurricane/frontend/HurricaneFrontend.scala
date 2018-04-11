package ch.epfl.labos.hurricane.frontend

import akka.actor._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app._
import ch.epfl.labos.hurricane.examples._
import com.typesafe.config._
import org.rogach.scallop._

class FrontendConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val me = opt[Int](default = None)
  val file = opt[String](default = Some("data"))
  val size = opt[Long](default = Some(1024 * 1024 * 1024))
  val textMode = opt[Boolean](default = Some(false))
  val genprops = props[String]('G')
  val hashprops = props[String]('H')
  val benchmark = trailArg[String]()
  val restartPhase = opt[Int](default = Some(0))
  verify()
}

object HurricaneFrontend {

  def main(args: Array[String]): Unit = {
    val arguments = new FrontendConf(args)

    val config =
      if (arguments.me.supplied) {
        //Config.HurricaneConfig.me = arguments.me()
        Config.HurricaneConfig.FrontendConfig.frontendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.netty.tcp.bind-hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.bind-port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.artery.bind.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes(arguments.me()).port))
      } else {
        Config.HurricaneConfig.FrontendConfig.frontendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.port))
      }

    implicit val system = ActorSystem("HurricaneFrontend", config)
    implicit val dispatcher = system.dispatcher
    implicit val materializer = akka.stream.ActorMaterializer()

    Config.HurricaneConfig.BackendConfig.NodesConfig.connectBackend(system)

    val app: HurricaneApplication =
      arguments.benchmark().trim.toLowerCase match {
        case "wordcount" => WordCount
        case "clicklog" => ClickLog
        case "hashjoin" => HashJoin
        case "chaos" => Chaos
        case "generator" => Generator
        case _ => Microbenchmarks
      }

    val appConf = AppConf.fromScallop(arguments)

    if(Config.HurricaneConfig.legacy) {

      // legacy mode
      val blueprint: Props = app.sequentialExecution(appConf)
      val control = system.actorOf(WorkExecutor.props(blueprint), WorkExecutor.name)

    } else {

      // dynamic mode
      if(Config.HurricaneConfig.me == Config.HurricaneConfig.AppConfig.appMasterId) {
        val phases = app.blueprints(appConf)
        system.actorOf(AppMaster.props(arguments.benchmark().trim.toLowerCase, app, appConf, arguments.restartPhase.toOption), AppMaster.name)
      }

      val scheduler = system.actorOf(TaskScheduler.props(app), TaskScheduler.name)

    }

    system.registerOnTermination {
      System.exit(0)
    }

  }

  case class Broadcast(req: Any)

  case class RequestWrapper(req: Any, sender: ActorRef, retries: Int = 3)

  case class RequestAborted(req: Any, reason: Throwable)

  case object Processed

}
