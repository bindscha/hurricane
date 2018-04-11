package ch.epfl.labos.hurricane

package object common {

  implicit class RichChunk(val array: Array[Byte]) extends AnyVal with Chunk

}
