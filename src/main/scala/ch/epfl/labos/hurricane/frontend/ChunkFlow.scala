/*
 * Copyright (c) 2018 EPFL IC LABOS.
 * 
 * This file is part of Hurricane
 * (see https://labos.epfl.ch/hurricane).
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package ch.epfl.labos.hurricane.frontend

import akka._
import akka.stream._
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.serialization.Format

import scala.collection.immutable.Seq
import scala.concurrent._

// Some of this can probably be factorized and cleaned up

trait ChunkFlow[In, Mat] {
  def flow: Flow[In, Chunk, Mat]

  def cMap[A: Format, B: Format](f: A => B): Flow[In, Chunk, Mat] = flow.via(HurricaneGraph.map[A, B](f))

  def cTransform[A: Format, B: Format](f: Iterator[A] => Iterator[B]): Flow[In, Chunk, Mat] = flow.via(HurricaneGraph.transform[A, B](f))

  def cSplitBy[A: Format](k: Int, f: A => Int): Flow[In, (FlowIndex, Chunk), Mat] = flow.via(HurricaneGraph.splitBy(k, f))

  def cGroupBy[A: Format, B: Format](state: GroupByState[A, B], valueParser: Option[A => (A, B)] = None): Flow[In, Chunk, Mat] = flow.via(HurricaneGraph.groupBy[A, B](state, valueParser))

  def cJoin[A: Format, B: Format](state: JoinState[A, B]): Flow[In, Chunk, Mat] = flow.via(HurricaneGraph.join[A, B](state))

}

trait ChunkSource[Mat] {
  def flow: Source[Chunk, Mat]

  def cMap[A: Format, B: Format](f: A => B): Source[Chunk, Mat] = flow.via(HurricaneGraph.map[A, B](f))

  def cTransform[A: Format, B: Format](f: Iterator[A] => Iterator[B]): Source[Chunk, Mat] = flow.via(HurricaneGraph.transform[A, B](f))

  def cSplitBy[A: Format](k: Int, f: A => Int): Source[(FlowIndex, Chunk), Mat] = flow.via(HurricaneGraph.splitBy(k, f))

  def cGroupBy[A: Format, B: Format](state: GroupByState[A, B], valueParser: Option[A => (A, B)] = None): Source[Chunk, Mat] = flow.via(HurricaneGraph.groupBy[A, B](state, valueParser))

  def cJoin[A: Format, B: Format](state: JoinState[A, B]): Source[Chunk, Mat] = flow.via(HurricaneGraph.join[A, B](state))

  def toBag(bag: Bag)(implicit dispatcher: ExecutionContext): RunnableGraph[Future[Done]] = flow.toMat(HurricaneGraph.sink(bag))(Keep.right)
}

trait ChunkSink[Mat] {
  def sink: Sink[Chunk, Mat]
}

trait IndexedChunkFlow[In, Mat] {
  def flow: Flow[In, (FlowIndex, Chunk), Mat]

  def toBags(bags: Seq[Bag])(implicit dispatcher: ExecutionContext): Sink[In, Future[Done]] = HurricaneGraph.flowToSinks(flow, bags)
}

trait IndexedChunkSource[Mat] {
  def flow: Source[(FlowIndex, Chunk), Mat]

  def toBags(bags: Seq[Bag])(implicit dispatcher: ExecutionContext): RunnableGraph[Future[Done]] = HurricaneGraph.sourceToSinks(flow, bags)
}

trait IndexedChunkSink[Mat] {
  def sink: Sink[(FlowIndex, Chunk), Mat]
}

object ParSource {
  def multi[Out, Mat](k: Int)(source: () => Source[Out, Mat]): Source[Out, NotUsed] =
    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val srcs = (0 until k) map (_ => builder.add(source().async))
      val merge = builder.add(Merge[Out](k))

      for(i <- 0 until k) srcs(i).out ~> merge.in(i)

      SourceShape(merge.out)
    })
}

trait ParSource[+Out, +Mat] {
  def source: Source[Out, Mat]

  def parallelize[Out2, Mat2](k: Int)(flow2: Flow[Out, Out2, Mat2]): Source[Out2, NotUsed] = Source.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val src = builder.add(source)
    val dispatch = builder.add(Balance[Out](k))
    val merge = builder.add(Merge[Out2](k))

    src.out ~> dispatch
    for(i <- 0 until k) dispatch.out(i) ~> flow2.async ~> merge.in(i)

    SourceShape(merge.out)
  })
}

trait ParFlow[-In, +Out, +Mat] {
  def flow: Flow[In, Out, Mat]

  def parallelize[Out2,Mat2](k: Int)(flow2: Flow[Out, Out2, _]): Flow[In, Out2, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val src = builder.add(flow)
    val dispatch = builder.add(Balance[Out](k))
    val merge = builder.add(Merge[Out2](k))

    src.out ~> dispatch
    for(i <- 0 until k) dispatch.out(i) ~> flow2.async ~> merge.in(i)

    FlowShape(src.in, merge.out)
  })
}
