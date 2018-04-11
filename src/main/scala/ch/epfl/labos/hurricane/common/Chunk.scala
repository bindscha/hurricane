package ch.epfl.labos.hurricane.common

import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.serialization._
import ch.epfl.labos.hurricane.util._
import java.nio._

object Chunk {

  val dataSize: Int = Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize

  val metaSize: Int = Config.HurricaneConfig.BackendConfig.DataConfig.metaSize

  val size: Int = dataSize + metaSize

  def wrap(bytes: Array[Byte]): Chunk = new RichChunk(bytes)

}

object ChunkMeta {

  val chunkSizeSize: Int = Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeSize
  val cmdSize: Int = Config.HurricaneConfig.BackendConfig.DataConfig.cmdSize
  val bagSize: Int = Config.HurricaneConfig.BackendConfig.DataConfig.bagSize

  val chunkSizeOffset: Int = Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
  val cmdOffset: Int = Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize + chunkSizeSize
  val bagOffset: Int = Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize + chunkSizeSize + cmdSize

}

trait ChunkMeta extends Any {
  this: Chunk =>

  import ChunkMeta._

  def chunkSize(size: Int): Unit = {
    val buf = ByteBuffer.wrap(array, chunkSizeOffset, chunkSizeSize)
    buf.putInt(size)
  }

  def chunkSize: Int = {
    val buf = ByteBuffer.wrap(array, chunkSizeOffset, chunkSizeSize)
    buf.getInt
  }

  def cmd(cmd: String): Unit = {
    cmdOffset until cmdOffset + cmdSize foreach (i => array(i) = 0)
    val bytes = cmd.getBytes
    val length = if(bytes.length < cmdSize) bytes.length else cmdSize
    System.arraycopy(bytes, 0, array, cmdOffset, length)
  }

  def cmd: String = {
    val ret = new Array[Byte](cmdSize)
    System.arraycopy(array, cmdOffset, ret, 0, cmdSize)
    new String(ret).trim
  }

  def bag(bag: Bag): Unit = {
    bagOffset until bagOffset + bagSize foreach (i => array(i) = 0)
    val bytes = bag.id.getBytes
    val length = if(bytes.length < bagSize) bytes.length else bagSize
    System.arraycopy(bytes, 0, array, bagOffset, length)
  }

  def bag: Bag = {
    val ret = new Array[Byte](bagSize)
    System.arraycopy(array, bagOffset, ret, 0, bagSize)
    Bag(new String(ret).trim)
  }

}

trait Chunk extends Any with ChunkMeta {

  def array: Array[Byte]

  def asByteBuffer: ByteBuffer = ByteBuffer.wrap(array, 0, chunkSize)

  def iterator[A: Format]: Iterator[A] = implicitly[Format[A]].iterator(asByteBuffer)

  def pusher[A: Format]: Pusher[A] = implicitly[Format[A]].pusher(asByteBuffer)

}
