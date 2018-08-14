package ch.epfl.labos.hurricane.backend

import akka.actor._
import akka.pattern._
import akka.util._
import better.files.File
import ch.epfl.labos.hurricane.Config.HurricaneConfig.NodeAddress
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.common._
import com.typesafe.config.ConfigValueFactory
import org.rogach.scallop.ScallopConf

import scala.collection.mutable.{Map => MMap}
import scala.concurrent.duration._
import scala.language.postfixOps

//twitter
import com.twitter.zk.ZkClient



object HurricaneBackendActor {

  val name: String = "master"

  def props(host: Option[String], subDirectory: Option[String]): Props = Props(classOf[HurricaneBackendActor], host, subDirectory)

}

class HurricaneBackendActor(host: Option[String], subDirectory: Option[String]) extends Actor with ActorLogging {

  import context.dispatcher

  implicit val timeout = Timeout(300 seconds)

  val rootFile = subDirectory match {
    case Some(sd) => File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath) / sd
    case _ => File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)
  }

  //zookeeper client
  implicit val timer = new com.twitter.util.JavaTimer(true)
  val connectTimeout = Some(com.twitter.util.Duration.fromSeconds(20))
  val sessionTimeout = com.twitter.util.Duration.fromSeconds(120)
  val zkClient = ZkClient("localhost:2181", connectTimeout, sessionTimeout)

  //Example address = 127.0.0.1:2551
  val address: String = host.getOrElse() + ":" + subDirectory.getOrElse()
  //println("xinjaguo storage id=" + address)

  val bags = MMap.empty[Bag, ActorRef]

  def receive = {
    case cmd: Command =>
      val senderId = (sender.path.address.host, sender.path.address.port) match {
        case (Some(host), Some(port)) => Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes.indexOf(NodeAddress(host, port))
        case _ => -1
      }
      

      (bagActor(cmd.bag) ? cmd) pipeTo sender
  }

  log.info(s"Started backend!")

  def bagActor(bag: Bag): ActorRef =
    bags getOrElseUpdate(bag, context.actorOf(HurricaneIO.props(bag, address, zkClient, rootFile)))

}

class BackendConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val me = opt[Int](default = None)

  verify()

}

object HurricaneBackend {

  def main(args: Array[String]): Unit = {
    val arguments = new BackendConf(args)

    val config =
      if (arguments.me.supplied) {
        //Config.HurricaneConfig.me = arguments.me()
        //println("xinjaguo hostname: " + ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname).)
        Config.HurricaneConfig.BackendConfig.backendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.netty.tcp.bind-hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.netty.tcp.bind-port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).port))
          .withValue("akka.remote.artery.bind.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname))
          .withValue("akka.remote.artery.bind.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).port))
      } else {
        Config.HurricaneConfig.BackendConfig.backendConfig
          .withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.port))
          .withValue("akka.remote.artery.canonical.hostname", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.hostname))
          .withValue("akka.remote.artery.canonical.port", ConfigValueFactory.fromAnyRef(Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.port))
      }

    val system = ActorSystem("HurricaneBackend", config)

    ch.epfl.labos.hurricane.frontend.Statistics.init(system.dispatchers.lookup("hurricane.backend.statistics-dispatcher"))

    val hostname =
      if (arguments.me.supplied) {
        Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).hostname
      } else {
        Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.hostname
      }

    val port =
      if (arguments.me.supplied) {
        Config.HurricaneConfig.BackendConfig.NodesConfig.nodes(arguments.me()).port
      } else {
        Config.HurricaneConfig.BackendConfig.NodesConfig.localNode.port
      }

    Config.ModeConfig.mode match {
      case Config.ModeConfig.Dev =>
        system.actorOf(HurricaneBackendActor.props(Some(hostname + ""), Some(port + "")), HurricaneBackendActor.name)
      case Config.ModeConfig.Prod =>
        system.actorOf(HurricaneBackendActor.props(None, None), HurricaneBackendActor.name)
    }
  }

}
