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

import akka._
import akka.actor._
import akka.pattern._
import akka.util._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.common._

import scala.concurrent._
import scala.concurrent.duration._

object HurricaneWork {

  case class Done[A](data: Option[A])

  case object Start

}

trait HurricaneWork extends Actor with ActorLogging {

  import HurricaneWork._

  type D <: Any

  override def preStart(): Unit = {
    super.preStart()

    log.info(s"$name created!")
  }

  override def postStop(): Unit = {
    stop()
    master ! Done(data)
    log.info(s"$name done!")

    super.postStop()
  }

  def master: ActorRef = context.parent

  def data: Option[D] = None

  def stop(): Unit = {}

  override def receive = {
    case Start =>
      log.info(s"$name started!")
      context become workerReceive
      start()

    case msg =>
      log.warning(s"$name received message $msg before start!")
  }

  def name: String = "Generic work"

  def start(): Unit = {}

  def workerReceive: Receive = Actor.emptyBehavior

}

object HurricaneRewind {

  val name: String = "rewind"

  def props(bag: Bag): Props = Props(classOf[HurricaneRewind], bag.id)

  case object Tick
  case class Acked(id: Int)

  implicit val timeout = Timeout(5.seconds)

}

class HurricaneRewind(bag: Bag) extends HurricaneWork {

  import HurricaneRewind._
  import context.dispatcher

  override val name = "Rewind " + bag.id

  val backend = Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs

  val fingerprint = java.util.UUID.nameUUIDFromBytes(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.toString.getBytes).toString

  val ready = Array.fill(backend.size)(false)

  var timer: Option[Cancellable] = None

  override def start(): Unit = {
    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HurricaneConfig.FrontendConfig.syncBarrierRetry, self, Tick))
  }

  override def stop(): Unit = {
    timer map (_.cancel())
  }

  override def workerReceive = {
    case Acked(id) if id >= 0 && id < ready.length =>
      ready(id) = true
      if (ready forall identity) { // received ready from all
        context stop self
      }

    case Tick =>
      ready.zipWithIndex filter (_._1 == false) foreach { p =>
        val id = p._2
        val me = self
        (backend(id) ? Rewind(fingerprint, bag)) map { case Ack => me ! Acked(id) }
      }
  }

}
object HurricaneNoop {

  val name: String = "noop"

  def props: Props = Props(classOf[HurricaneNoop])

}

class HurricaneNoop extends HurricaneWork {


  override val name = "Noop"

  override def workerReceive = Actor.emptyBehavior

}

object HurricaneTrunc {

  val name: String = "trunc"

  def props(bag: Bag): Props = Props(classOf[HurricaneTrunc], bag.id)

  case object Tick
  case class Acked(id: Int)

  implicit val timeout = Timeout(5.seconds)

}

class HurricaneTrunc(bag: Bag) extends HurricaneWork {

  import HurricaneTrunc._
  import context.dispatcher

  override val name = "Trunc " + bag.id

  val backend = Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs

  val fingerprint = java.util.UUID.nameUUIDFromBytes(Config.HurricaneConfig.FrontendConfig.NodesConfig.localNode.toString.getBytes).toString

  val ready = Array.fill(backend.size)(false)

  var timer: Option[Cancellable] = None

  override def start(): Unit = {
    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HurricaneConfig.FrontendConfig.syncBarrierRetry, self, Tick))
  }

  override def stop(): Unit = {
    timer map (_.cancel())
  }

  override def workerReceive = {
    case Acked(id) if id >= 0 && id < ready.length =>
      ready(id) = true
      if (ready forall identity) { // received ready from all
        context stop self
      }

    case Tick =>
      ready.zipWithIndex filter (_._1 == false) foreach { p =>
        val id = p._2
        val me = self
        (backend(id) ? Trunc(fingerprint, bag)) map { case Ack => me ! Acked(id) }
      }
  }

}

object HurricaneFrontendSync {

  val name: String = "sync"

  def props(clients: List[Config.HurricaneConfig.NodeAddress]): Props = Props(classOf[HurricaneFrontendSync], clients)

  case object Tick

}

class HurricaneFrontendSync(clients: List[Config.HurricaneConfig.NodeAddress]) extends HurricaneWork {

  import HurricaneFrontendSync._
  import context.dispatcher

  override val name = "Synchronization barrier"

  val peers = clients.map(na => self.path.toStringWithAddress(self.path.address.copy(protocol = Config.HurricaneConfig.protocol, host = Some(na.hostname), port = Some(na.port)))).map(context.actorSelection)

  val ready = Array.fill(peers.size)(false)

  var timer: Option[Cancellable] = None

  override def start(): Unit = {
    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HurricaneConfig.FrontendConfig.syncBarrierRetry, self, Tick))
  }

  override def stop(): Unit = {
    Statistics.reset() // Reset the stats
    timer map (_.cancel())
  }

  override def workerReceive = {
    case Ready(id) if id >= 0 && id < ready.length =>
      ready(id) = true
      if (ready forall identity) { // received ready from all
        peers foreach (_ ! Synced)
      }

    case Synced => // forward along the ring and exit
      context stop self

    case Tick =>
      ready.zipWithIndex filter (_._1 == false) foreach { p => peers(p._2) ! Ready(Config.HurricaneConfig.me) }
  }

}

import akka.stream._
import akka.stream.scaladsl._

object HurricaneGraphWork {

  val name: String = "control"

  def props(name: String)(graph: RunnableGraph[Future[Done]]): Props =
    Props(new HurricaneGraphWork(name)(graph))

}

class HurricaneGraphWork(override val name: String = "Graph work")(graph: RunnableGraph[Future[Done]]) extends HurricaneWork {

  import context.dispatcher

  override type D = Done

  implicit val materializer = ActorMaterializer()

  var data_ : Option[Done] = None

  override def data: Option[Done] = data_

  override def start(): Unit = {
    val me = self
    graph.mapMaterializedValue(_.onComplete(_ => me ! Done)).run()
  }

  override def workerReceive = {
    case Done =>
      data_ = Some(Done)
      context stop self
  }

}

object WorkExecutor {

  val name: String = "executor"

  def props(blueprint: Props): Props = Props(classOf[WorkExecutor], blueprint)

}

class WorkExecutor(blueprint: Props) extends Actor with ActorLogging {

  var terminationOK = false

  override def preStart(): Unit = {
    super.preStart()

    val worker = context.actorOf(blueprint, "worker")
    context.watch(worker)
    worker ! HurricaneWork.Start
    log.info("Execution started!")
    Statistics.start()
  }

  override def postStop(): Unit = {
    Statistics.stop()
    log.info("Execution done!")
    context.child("worker") map context.unwatch

    //log.info(Statistics.statsString) // Print the stats

    super.postStop()
  }

  override def receive = {
    case HurricaneWork.Done(data) =>
      terminationOK = true
      context stop self

    case Terminated(actor) =>
      context.child("worker") match {
        case Some(worker) if worker == actor && !terminationOK =>
          log.warning("Whoops! Worker got terminated before it was done! Badness ensues...")
          context stop self
        case _ => // ignore (either condition not matching is fine)
      }

  }

}

object SeqWorkExecutor {

  val name: String = "seq-executor"

  def props(blueprints: Seq[Props]): Props = Props(classOf[SeqWorkExecutor], blueprints)

}

// XXX: Current composition pattern does not allow for data to reach the outer seqexecutor...
// This should eventually be fixed, but it is not a priority
class SeqWorkExecutor(blueprints: Seq[Props]) extends Actor with ActorLogging {

  var steps = blueprints.zipWithIndex
  var currentExecutor: Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()

    currentExecutor = runNext(None)
    if (currentExecutor.isEmpty) {
      self ! HurricaneWork.Done(None)
    }
    log.info("Sequential execution started!")
  }

  protected def runNext(data: Option[Any]): Option[ActorRef] =
    steps match {
      case Nil =>
        None
      case (blueprint, index) :: bs =>
        steps = bs
        val executor = context.actorOf(blueprint, s"step${index + 1}")
        context watch executor
        data foreach (executor ! _)
        Some(executor)
    }

  override def postStop(): Unit = {
    log.info("Sequential execution done!")

    super.postStop()
  }

  def receive = {
    case HurricaneWork.Done(data) =>
      context stop self

    case Terminated(actor) /*if currentExecutor.contains(actor)*/ =>
      if (steps.nonEmpty) {
        context unwatch actor
        runNext(None)
      } else {
        context stop self
      }
  }

}
