package ch.epfl.labos.hurricane.util

import scala.reflect.ClassTag

trait RingLike[T] {
  def size: Int

  /** Put one element onto the tail position of the ring buffer. Returns None if failed. */
  def put(o: T): Option[T]

  /** Gets one element from the head position of the ring buffer. Returns None if failed. */
  def take: Option[T]
}

class CircularBuffer[T: ClassTag](val capacity: Int) extends RingLike[T] {
  assert(capacity > 0 && capacity < Int.MaxValue)

  // invariants:
  // * head     is equal to tail -> the buffer is empty
  // * (head+1) is equal to tail -> the buffer is full
  // * tail always points to a sentinel, which is necessarily free

  private val len = capacity + 1
  private val ring = new Array[T](len)
  private var head: Int = 0
  private var tail: Int = 0

  override def size: Int = if (head >= tail) head - tail else len - tail + head

  override def put(o: T): Option[T] = {
    var next = head + 1
    next = if (next >= len) 0 else next
    if (next == tail)
      None
    else {
      ring(head) = o
      head = next
      Some(o)
    }
  }

  override def take: Option[T] = {
    if (head == tail)
      None
    else {
      val o = ring(tail)
      var next = tail + 1
      tail = if (next >= len) 0 else next
      Some(o)
    }
  }
}