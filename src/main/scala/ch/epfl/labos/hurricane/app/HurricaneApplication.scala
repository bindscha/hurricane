package ch.epfl.labos.hurricane.app

import akka.actor.Props
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._

import scala.collection.immutable.Seq
import scala.concurrent._

trait HurricaneApplication {

  def phases(appConf: AppConf)(implicit dispatcher: ExecutionContext): Seq[Seq[Props]] =
    blueprints(appConf).map(phase => phase.map(blueprint => instantiate(blueprint)))

  def sequentialExecution(appConf: AppConf)(implicit dispatcher: ExecutionContext): Props =
    SeqWorkExecutor.props(phases(appConf).flatMap(p => Seq(WorkExecutor.props(HurricaneFrontendSync.props(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes)), phase2props(p))) :+ WorkExecutor.props(HurricaneFrontendSync.props(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes)))

  // XXX: does not work very well because we try to extract the info from the Props (which encapsulate things pretty well)
  def sequentialExecutionDescription(appConf: AppConf)(implicit dispatcher: ExecutionContext): String =
    (for {
      (phase, i) <- phases(appConf).zipWithIndex
    } yield { s"\nPhase $i:\n" + phase.map(_.args.lastOption.map("- " + _) getOrElse "- (no description)").mkString("\n") }) mkString "\n"

  private def phase2props(phase: Seq[Props]): Props = {
    filterWork(phase).toList match {
      case Nil => WorkExecutor.props(HurricaneNoop.props)
      case w :: Nil => WorkExecutor.props(w)
      case ws => SeqWorkExecutor.props(ws map WorkExecutor.props)
    }
  }

  private val m = Config.HurricaneConfig.FrontendConfig.NodesConfig.machines
  private val me = Config.HurricaneConfig.me

  protected def filterWork(seq: Seq[Props]): Seq[Props] = seq.indices.filter(_ % m == me) map seq


  // New API

  def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props

  def blueprints(appConf: AppConf): Seq[Seq[Blueprint]]

  def merge(phase: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag]): Option[Blueprint] = None

}
