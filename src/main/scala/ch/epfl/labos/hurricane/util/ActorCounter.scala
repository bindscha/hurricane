package ch.epfl.labos.hurricane.util

import akka.actor._

object ActorCounter {
  type Id = Long
}

trait ActorCounter {
  this: Actor =>

  import ActorCounter._

  private var counter: Id = 0

  def nextId: Id = {
    counter += 1
    counter
  }

}
