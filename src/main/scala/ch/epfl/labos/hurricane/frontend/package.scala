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
