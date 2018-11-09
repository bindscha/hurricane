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
package ch.epfl.labos.hurricane

import akka.actor.{ActorSelection, ActorSystem}
import ch.epfl.labos.hurricane.util._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.language.postfixOps

trait ConfigUtils { // XXX: should go somewhere else
  import com.typesafe.config._

  implicit class ImprovedConfig(underlying: Config) {
    def getConfigOpt(path: String): Option[Config] =
      if (underlying.hasPath(path)) Some(underlying.getConfig(path))
      else None

    def getIntOpt(path: String): Option[Int] =
      if (underlying.hasPath(path)) Some(underlying.getInt(path))
      else None

    def getLongOpt(path: String): Option[Long] =
      if (underlying.hasPath(path)) Some(underlying.getLong(path))
      else None

    def getDoubleOpt(path: String): Option[Double] =
      if (underlying.hasPath(path)) Some(underlying.getDouble(path))
      else None

    def getStringOpt(path: String): Option[String] =
      if (underlying.hasPath(path)) Some(underlying.getString(path))
      else None
  }

}

object Config extends ConfigUtils {

  import com.typesafe.config.ConfigFactory

  private val config = ConfigFactory.load()

  private val root = config.getConfig("hurricane")

  object ModeConfig {

    val mode = root.getStringOpt("mode").map(_.trim.toLowerCase) match {
      case Some("dev") => Dev
      case _ => Prod
    }

    sealed trait Mode

    case object Dev extends Mode

    case object Prod extends Mode
  }

  object HurricaneConfig {

    val me = root.getInt("me")

    val protocol = root.getString("protocol")

    val legacy = root.getBoolean("legacy")

    val legacyHeuristic = root.getBoolean("legacy-heuristic")

    private val NodeAddressPattern = """([^:]+):(\d+)""".r

    case class NodeAddress(hostname: String, port: Int)

    object BackendConfig {
      val backendConfig = root.getConfig("backend")
      private val NodeAddressPattern = """([^:]+):(\d+)""".r

      case class NodeAddress(hostname: String, port: Int)

      object DataConfig {

        val ioEngine: IOEngine = backendConfig.getString("io-engine").trim.toLowerCase match {
          case "direct" | "direct-io" | "direct_io" | "direct io" | "dio" => DirectIOEngine
          case _ => DefaultIOEngine
        }
        val dataDirectory = new java.io.File(backendConfig.getStringOpt("data-dir").map(_.trim.toLowerCase).getOrElse("."))
        val chunkSize = backendConfig.getBytes("chunk-size").toInt
        val chunkSizeMiB = chunkSize >> 20

        val chunkSizeSize = backendConfig.getBytes("meta.chunk-size").toInt
        val cmdSize = backendConfig.getBytes("meta.cmd-size").toInt
        val bagSize = backendConfig.getBytes("meta.bag-size").toInt
        val metaSize = chunkSizeSize + cmdSize + bagSize

        sealed trait IOEngine

        case object DefaultIOEngine extends IOEngine

        case object DirectIOEngine extends IOEngine
      }

      object NodesConfig {
        val nodes = backendConfig.getList("nodes").unwrapped.toArray.toList.map(_.toString) map {
          case NodeAddressPattern(hostname, port) => NodeAddress(hostname, port.toInt)
        }

        val machines = nodes.size

        val localNode = nodes(me)

        import akka.actor._

        import scala.concurrent._
        import scala.concurrent.duration._

        var backendRefs: Seq[ActorRef] = Seq.empty

        def connectBackend(implicit system: ActorSystem): Unit = {
          import system.dispatcher
          backendRefs = Await.result(Future.sequence(nodes.map(na => system.actorSelection(s"$protocol://HurricaneBackend@${na.hostname}:${na.port}/user/master").resolveOne(30 seconds))), 30 seconds)
        }
      }

    }

    object FrontendConfig {
      sealed trait IOMode

      case object Spreading extends IOMode

      case object InputLocal extends IOMode

      case object OutputLocal extends IOMode

      val frontendConfig = root.getConfig("frontend")
      val batchingFactor = if(frontendConfig.getInt("force-batching-factor") > 0) frontendConfig.getInt("force-batching-factor") else math.min(frontendConfig.getInt("batching-factor"), BackendConfig.NodesConfig.machines)
      val sourceBufferSize = frontendConfig.getInt("source-buffer")
      val sourceRetry: FiniteDuration = frontendConfig.getDuration("source-retry")
      val sinkQueueSize = frontendConfig.getInt("sink-queue")
      val sinkRetry: FiniteDuration = frontendConfig.getDuration("sink-retry")
      val syncBarrierRetry: FiniteDuration = frontendConfig.getDuration("sync-barrier-retry")
      val schedulerTick: FiniteDuration = frontendConfig.getDuration("scheduler-tick")
      val cloningThreshold: Double = frontendConfig.getDouble("cloning-threshold")
      val cloningTime: FiniteDuration = frontendConfig.getDuration("cloning-time")
      val cloningEnabled: Boolean = frontendConfig.getBoolean("cloning-enabled")
      val ioMode: IOMode = frontendConfig.getString("io-mode").trim.toLowerCase match {
        case "input-local" => InputLocal
        case "output-local" => OutputLocal
        case _ => Spreading
      }

      val parallelism: Int = frontendConfig.getInt("parallelism")

      val chunkPoolSize = frontendConfig.getBytes("chunk-pool-size").toLong

      object NodesConfig {
        val nodes = frontendConfig.getList("nodes").unwrapped.toArray.toList.map(_.toString) map {
          case NodeAddressPattern(hostname, port) => NodeAddress(hostname, port.toInt)
        }

        val machines = nodes.size

        val localNode = nodes(me)
      }

    }

    object AppConfig {
      val appConfig = root.getConfig("app")

      val appMasterId = appConfig.getInt("master.id")

      val appMaster = FrontendConfig.NodesConfig.nodes(appMasterId)

      def appMasterSelection(implicit system: ActorSystem): ActorSelection = {
        system.actorSelection(s"$protocol://HurricaneFrontend@${appMaster.hostname}:${appMaster.port}/user/master")
      }
    }

    object HdfsConfig {
      val hdfsConfig = root.getConfig("hdfs")

      val username = hdfsConfig.getString("username")
      val hadoop_home:String = hdfsConfig.getString("hadoop_home")
      val replication_factor: Short = hdfsConfig.getInt("replication_factor").toShort
    }

  }

}