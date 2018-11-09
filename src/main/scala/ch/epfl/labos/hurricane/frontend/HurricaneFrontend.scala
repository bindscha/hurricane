/*
 * Copyright (c) 2018 EPFL IC LABOS.
 * 
 * This file is part of Hurricane
 * (see https://labos.epfl.ch/hurricane).
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ch.epfl.labos.hurricane.frontend

import akka.actor._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app._
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
  val simulation = opt[Int](default = Some(0))
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

    import scala.reflect.runtime.{universe => ru}
    val rm = ru.runtimeMirror(getClass.getClassLoader)

    val app: HurricaneApplication =
      rm.reflectModule(rm.staticModule(arguments.benchmark().trim)).instance.asInstanceOf[HurricaneApplication]

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
