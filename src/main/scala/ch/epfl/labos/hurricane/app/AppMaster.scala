package ch.epfl.labos.hurricane.app

import akka.actor._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend.TaskScheduler
import io.jvm.uuid._

import scala.collection.mutable
import scala.concurrent.duration._
import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean

object AppMaster {

  val name: String = "master"

  def props(name: String, app: HurricaneApplication, appConf: AppConf, restartPhase: Option[Int]): Props = Props(classOf[AppMaster], name, app, appConf, restartPhase)

  private case object CloneWorker
  private case object KillWorker

}

class AppMaster(name: String, app: HurricaneApplication, appConf: AppConf, restartPhase: Option[Int]) extends Actor with ActorLogging {

  import AppMaster._
  import context.dispatcher

  val appDescription: Seq[Map[String, Blueprint]] = restartPhase match {
    case Some(i) => app.blueprints(appConf).drop(i).map(_.map(p => UUID.randomString -> p).toMap)
    case _ => app.blueprints(appConf).map(_.map(p => UUID.randomString -> p).toMap)
  }

  val clones = mutable.Map.empty[String, Blueprint]
  val cloneParents = mutable.Map.empty[String, String]
  val cloneCounts = mutable.Map.empty[String, Int]
  val workBag = WorkBag()
  val mergesBag = mutable.Map.empty[String, Blueprint]
  var currentPhase = restartPhase match { case Some(i) => i - 1; case _ => -1 }
  var mergeDone = true

  var count = 0

  var timestamps: Seq[Long] = Seq.empty

  val osBean = ManagementFactory.getPlatformMXBean(classOf[OperatingSystemMXBean])

  // Simulation
  var clonedTask = ""
  var scheduler: Option[ActorRef] = None

  override def preStart(): Unit = {
    super.preStart()

    if(appConf.simulation == 1) {
      log.info(s"Simulating failure scenario 1...")
      context.system.scheduler.scheduleOnce(1 seconds, self, CloneWorker)
      context.system.scheduler.scheduleOnce(10 seconds, self, KillWorker)
    }

    log.info(s"Application ${name} started!")
    gotoNextPhase()

    timestamps :+= System.currentTimeMillis
  }

  override def postStop(): Unit = {
    timestamps :+= System.currentTimeMillis

    val phaseT = timestamps.tail.zip(timestamps).map(tp => (tp._1 - tp._2)).tail.dropRight(1).zipWithIndex
    val totalT = (timestamps.last - timestamps.head)

    log.info(
      s"""Application ${name} completed!
         | Total time: $totalT ms
         | Phases: ${phaseT.map(t => s"${t._2 + 1} -> ${t._1} ms").mkString(", ")}
       """.stripMargin)

    super.postStop()
  }

  override def receive = {
    case GetTask =>
      workBag.ready.headOption match {
        case Some(id) =>
          blueprint(id) match {
            case Some(blueprint) =>
              sender ! Task(id, blueprint)
              workBag.running += id
              workBag.ready -= id
            case _ => // ignore
          }

        case _ => //XXX: reset tasks not done
          count += 1
          if(count >= 64) {
            workBag.ready ++= workBag.running
            workBag.running.clear()
            count = 0
          }
      }

    case TaskCompleted(id) if workBag.running.contains(id) =>
      workBag.running -= id
      workBag.done += id
      sender ! Ack

      if(workBag.ready.isEmpty && workBag.running.isEmpty) {
        gotoNextPhase()

        if(currentPhase >= appDescription.size) {
          // Shutdown schedulers
          Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes.map(na => self.path.parent.child(TaskScheduler.name).toStringWithAddress(self.path.address.copy(protocol = Config.HurricaneConfig.protocol, host = Some(na.hostname), port = Some(na.port)))).map(context.actorSelection) foreach (_ ! PoisonPill)
        } else {
          log.info(s"Moving on to phase ${currentPhase + 1}")
        }
      }

    case PleaseClone(id) if Config.HurricaneConfig.FrontendConfig.cloningEnabled =>
      blueprint(id) match {
        case Some(blueprint) =>
          val parent = cloneParents getOrElse (id, id)
          val nClones = cloneCounts getOrElseUpdate (parent, 1)

          if(nClones < Config.HurricaneConfig.FrontendConfig.NodesConfig.machines) {
            log.info(s"Accepted request for clone on task $id")
            val newUUID = UUID.randomString
            clones += newUUID -> (if(blueprint.needsMerge) blueprint.copy(outputs = blueprint.outputs.map(b => b.copy(id = b.id + s".clone$nClones"))) else blueprint)
            cloneParents += newUUID -> parent
            cloneCounts += parent -> (cloneCounts(parent) + 1)
            workBag.ready += newUUID
          } else {
            log.info(s"Rejected request for clone on task $id")
          }

        case _ => // ignore
      }

    // Simulation commands
    case CloneWorker =>
      workBag.running.headOption orElse workBag.ready.headOption match {
        case Some(id) =>
          clonedTask = id
          self ! PleaseClone(id)
      }

    case KillWorker =>
      // Kill clonedTask
      scheduler foreach (_ ! TaskScheduler.Restart)
      workBag.running -= clonedTask
      workBag.ready -= clonedTask
      val parent = cloneParents getOrElse (clonedTask, clonedTask)
      val nClones = cloneCounts getOrElseUpdate (parent, 1)
      clones -= clonedTask
      cloneParents -= clonedTask
      cloneCounts += parent -> (cloneCounts(parent) - 1)

      // Recreate worker
      val newUUID = UUID.randomString
      blueprint(clonedTask) match {
        case Some(blueprint) =>
          clones += newUUID -> (if(blueprint.needsMerge) blueprint.copy(outputs = blueprint.outputs.map(b => b.copy(id = b.id + s".clone$nClones"))) else blueprint)
          cloneParents += newUUID -> parent
          cloneCounts += parent -> (cloneCounts(parent) + 1)
          workBag.ready += newUUID

          // Replaying
          Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs foreach { ref =>
            ref ! Replay(clonedTask, newUUID, blueprint.inputs.head)
          }
      }
  }

  def gotoNextPhase(): Unit = {
    if(mergeDone) {
      var nextPhase: Iterable[String] = null
      do {
        currentPhase += 1
        nextPhase = try { appDescription(currentPhase).keys } catch { case _: Throwable => Iterable.empty[String]}
      } while(nextPhase.isEmpty && currentPhase < appDescription.size)

      timestamps :+= System.currentTimeMillis

      workBag.ready ++= nextPhase

      mergeDone = false
    } else {
      mergeDone = true
      val merges = cloneCounts map { case (id, n) => blueprint(id).get -> ((id, n)) } filter (_._1.needsMerge) map { case (b, (id, n)) => val out = b.outputs.head; val inputs = out +: (1 to n).map(i => out.copy(id = out.id + s".clone$i")); out -> ((id, inputs)) }
      val todo = merges.toList.flatMap(io => app.merge("", appConf, io._2._2, List(io._1)).map(t => io._2._1 -> t))
      clones.clear()
      cloneParents.clear()
      cloneCounts.clear()
      if(todo.isEmpty) {
        gotoNextPhase()
      } else {
        todo foreach { case (id, t) => workBag.ready += id; mergesBag += id -> t }
      }
    }
  }

  def blueprint(id: String): Option[Blueprint] =
    (appDescription(currentPhase) get id) orElse (clones get id)

}
