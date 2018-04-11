package ch.epfl.labos.hurricane.util

import scala.collection.immutable.Seq
import scala.util.Random

object Cyclic {

  def apply[A](seed: Int, nodes: Seq[A]): Cyclic[A] =
    new Cyclic[A](seed, nodes)

}

class Cyclic[A](seed: Int, nodes: Seq[A]) {

  val permutation = new Random(3 * seed).shuffle(new Random(seed).shuffle(nodes)) // XXX: need to shuffle twice with this stupid RNG
  val size = permutation.size

  private var i = 0

  def next: A = {
    val ret = permutation(i)
    i = (i + 1) % size
    ret
  }

}
