package ch.epfl.labos.hurricane.common

import io.jvm.uuid._

case class Bag(id: String) extends AnyVal

object Bag {

  def random: Bag = Bag(UUID.random.toString)

  def derived(seed: String): Bag = Bag(UUID.fromString(seed).toString)

}