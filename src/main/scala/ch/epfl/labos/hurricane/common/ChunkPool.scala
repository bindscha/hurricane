package ch.epfl.labos.hurricane.common

object ChunkPool {

  def allocate(): Chunk = {
    val ret = new RichChunk(new Array(Chunk.size))
    ret.chunkSize(Chunk.dataSize)
    ret
  }

  def deallocate(chunk: Chunk): Unit = {}

}
