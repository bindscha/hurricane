package ch.epfl.labos.hurricane.serialization

import java.nio.ByteBuffer

trait RichByteBuffer[I] extends Any {
  def buf: ByteBuffer

  def serializeTo(item: I): Unit
  def deserializeFrom(): I
}
