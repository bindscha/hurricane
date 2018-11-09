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
package ch.epfl.labos.hurricane.common

import ch.epfl.labos.hurricane.Config

object ChunkPool extends ChunkPool {

  override protected val nchunks: Int = (Config.HurricaneConfig.FrontendConfig.chunkPoolSize / Chunk.dataSize).toInt

}


trait ChunkPool {

  protected def nchunks: Int

  protected var chunks: List[Chunk] = Nil

  def init(): Unit = {
    chunks = (for(i <- 0 until nchunks) yield newChunk()).toList
  }

  def allocate(): Chunk = chunks match {
    case chunk :: cs => chunks = cs; chunk
    case _ => newChunk()
  }

  def deallocate(chunk: Chunk): Unit = {
    java.util.Arrays.fill(chunk.array, 0.toByte)
    chunks = chunk :: chunks
  }

  def newChunk(): Chunk = {
    val ret = new RichChunk(new Array(Chunk.size))
    ret.chunkSize(Chunk.dataSize)
    ret
  }

  def available: Int = chunks.size

}