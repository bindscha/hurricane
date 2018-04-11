package ch.epfl.labos.hurricane.app

import ch.epfl.labos.hurricane.common.Bag

import scala.collection.immutable.Seq

object Blueprint {
  def apply(name: String, appConf: AppConf, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq.empty, Seq.empty, needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), Seq.empty, needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, output: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), Seq(output), needsMerge)
  def apply(name: String, appConf: AppConf, inputs: Seq[Bag], output: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, inputs, Seq(output), needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, outputs: Seq[Bag], needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), outputs, needsMerge)
}

case class Blueprint(name: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag], needsMerge: Boolean)
