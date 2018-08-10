package ch.epfl.labos.hurricane.frontend

import java.lang.management.ManagementFactory

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.app.{Blueprint, HurricaneApplication}
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.util.Cyclic
import com.sun.management.OperatingSystemMXBean

import scala.concurrent.duration._

object TaskScheduler {

  val name: String = "scheduler"

  def props(app: HurricaneApplication): Props = Props(classOf[TaskScheduler], app, ProgressBasedCloningStrategy(0.8))

  case object Tick

  case object FetchTask
  case object MoveOn
  case class NotifyAgain(id: String)

}

class TaskScheduler(app: HurricaneApplication, cloningStrategy: CloningStrategy = ProgressBasedCloningStrategy(0.8)) extends Actor with ActorLogging {

  import TaskScheduler._
  import context.dispatcher

  implicit val timeout = Timeout(2.seconds)

  val fingerprint = java.util.UUID.nameUUIDFromBytes("master".getBytes).toString

  var timer: Option[Cancellable] = None
  var worker: Option[ActorRef] = None
  var currentBlueprint: Option[Blueprint] = None

  var startT: Long = 0

  val appMaster = Config.HurricaneConfig.AppConfig.appMasterSelection(context.system)
  val cyclic = Cyclic[ActorRef](42 * Config.HurricaneConfig.me, Config.HurricaneConfig.BackendConfig.NodesConfig.backendRefs)

  val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
  var lastOverload: Long = 0
  var lastLoad: Double = 0.0

  override def preStart(): Unit = {
    super.preStart()
    log.info("Task scheduler started!")

    timer = Some(context.system.scheduler.schedule(Duration.Zero, Config.HurricaneConfig.FrontendConfig.schedulerTick, self, Tick))
    self ! FetchTask
  }

  override def postStop(): Unit = {
    log.info("Task scheduler stopped!")
    timer map (_.cancel())

    context.system.terminate()

    super.postStop()
  }

  override def receive = {
    case Tick =>
      lastLoad = math.min(os.getProcessCpuLoad * 2, 1) // *2 adjusts for hyperthreading
      
      if(lastLoad > Config.HurricaneConfig.FrontendConfig.cloningThreshold) {
        if(lastOverload == 0) {
          lastOverload = System.currentTimeMillis
        } else if(System.currentTimeMillis - lastOverload > Config.HurricaneConfig.FrontendConfig.cloningTime.toMillis) {
          currentBlueprint flatMap (_.inputs.headOption) foreach (bag => cyclic.next ! Progress(fingerprint, bag))
        }
      } else {
        lastOverload = 0
      }

    case ProgressReport(done, size) =>
      if(cloningStrategy.offer(lastLoad, 0, done)) {
        context.children.headOption.foreach(ref => appMaster ! PleaseClone(ref.path.name))
        lastOverload = 0 // reset
        lastLoad = 0.0
      }

    case Task(id, blueprint) =>
      val props = app.instantiate(blueprint)
      worker = Some(context.actorOf(WorkExecutor.props(props), id))
      currentBlueprint = Some(blueprint)
      lastOverload = 0
      worker map context.watch
      log.info("Task started: " + id)
      startT = System.currentTimeMillis

    case Terminated(actor) =>
      worker match {
        case Some(w) if w == actor =>
          val time = System.currentTimeMillis - startT
          context unwatch w
          log.info(s"Task completed in $time ms: ${actor.path.name}")

          notifyComplete(actor.path.name)

        case _ =>
          log.warning(s"Unknown worker $actor terminated! Ignoring...")
      }

    case MoveOn =>
      worker = None
      currentBlueprint = None
      self ! FetchTask

    case NotifyAgain(id) =>
      notifyComplete(id)

    case FetchTask =>
      if(worker.isEmpty) {
        fetchTask()
      } else {
        log.info("A worker is already running, not fetching a new task just yet...")
      }

  }

  def fetchTask(): Unit = {
    val me = self
    (appMaster ? GetTask) onComplete {
      case scala.util.Success(t: Task) => me ! t
      case _ => me ! FetchTask
    }
  }

  def notifyComplete(id: String): Unit = {
    val me = self
    (appMaster ? TaskCompleted(id)) onComplete {
      case scala.util.Success(Ack) => me ! MoveOn
      case _ => me ! NotifyAgain(id)
    }
  }

}
