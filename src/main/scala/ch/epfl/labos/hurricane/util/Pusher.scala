package ch.epfl.labos.hurricane.util

trait Pusher[A] {

  def pushed: Int
  def hasRemaining: Boolean
  def put(something: A): Int

}
