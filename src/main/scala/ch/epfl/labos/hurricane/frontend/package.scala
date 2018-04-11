package ch.epfl.labos.hurricane

import akka._
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane.common._

import scala.concurrent._
import scala.language.implicitConversions

package object frontend {

  type FlowIndex = Int

  implicit def bag2source(bag: Bag)(implicit dispatcher: ExecutionContext): Source[Chunk, Future[Done]] = HurricaneGraph.source(bag)(dispatcher)

  implicit def bag2sink(bag: Bag)(implicit dispatcher: ExecutionContext): Sink[Chunk, Future[Done]] = HurricaneGraph.sink(bag)(dispatcher)

  implicit def bag2csource(bag: Bag)(implicit dispatcher: ExecutionContext): ChunkSource[Future[Done]] = new ChunkSourceImpl(bag2source(bag)(dispatcher))

  implicit def bag2psource(bag: Bag)(implicit dispatcher: ExecutionContext): ParSource[Chunk, Future[Done]] = new ParSourceImpl(bag2source(bag)(dispatcher))

  implicit class ChunkSourceImpl[Mat](override val flow: Source[Chunk, Mat]) extends ChunkSource[Mat]

  implicit class IndexedChunkSourceImpl[Mat](override val flow: Source[(FlowIndex, Chunk), Mat]) extends IndexedChunkSource[Mat]

  implicit class ChunkFlowImpl[In, Mat](override val flow: Flow[In, Chunk, Mat]) extends ChunkFlow[In, Mat]

  implicit class IndexedChunkFlowImpl[In, Mat](override val flow: Flow[In, (FlowIndex, Chunk), Mat]) extends IndexedChunkFlow[In, Mat]

  implicit class ParSourceImpl[Out, Mat](override val source: Source[Out, Mat]) extends ParSource[Out, Mat]

  implicit class ParFlowImpl[In, Out, Mat](override val flow: Flow[In, Out, Mat]) extends ParFlow[In, Out, Mat]

}
