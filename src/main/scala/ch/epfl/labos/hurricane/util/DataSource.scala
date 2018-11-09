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
package ch.epfl.labos.hurricane.util

import akka.NotUsed
import akka.stream.scaladsl._
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.common.{Chunk, ChunkPool}
import com.thedeanda.lorem._
import it.unimi.dsi.util.XoRoShiRo128PlusRandom
import org.apache.commons.math3.distribution._

import scala.language.postfixOps

object DataSource {

  protected def zeroIterator: Iterator[Chunk] =
    Iterator.continually(ChunkPool.allocate())

  def zero: Source[Chunk, NotUsed] =
    Source.fromIterator(() => zeroIterator)

  def zero(size: Long): Source[Chunk, NotUsed] =
    Source.fromIterator(() => cutIterator(zeroIterator, size))

  protected def randomIterator: Iterator[Chunk] = {
    val random = new XoRoShiRo128PlusRandom
    Iterator.continually {
      val ret = ChunkPool.allocate()
      val buf = ret.asByteBuffer
      while(buf.hasRemaining) {
        buf.putLong(random.nextLong)
      }
      ret
    }
  }

  def random: Source[Chunk, NotUsed] = {
    Source.fromIterator(() => randomIterator)
  }

  def random(size: Long): Source[Chunk, NotUsed] =
    Source.fromIterator(() => cutIterator(randomIterator, size))

  protected def loremIterator: Iterator[Chunk] = {
    val lorem = LoremIpsum.getInstance()
    Iterator.continually {
      val ret = ChunkPool.allocate()
      val buf = ret.asByteBuffer
      while(buf.hasRemaining) {
        val text = lorem.getWords(1000).getBytes
        if(buf.remaining >= text.length) {
          buf.put(text)
        } else {
          buf.put(text.take(buf.remaining))
        }
      }
      ret
    }
  }

  def lorem: Source[Chunk, NotUsed] = {
    Source.fromIterator(() => loremIterator)
  }

  def lorem(size: Long): Source[Chunk, NotUsed] =
    Source.fromIterator(() => cutIterator(loremIterator, size))

  protected def commonsIterator(distribution: IntegerDistribution): Iterator[Chunk] = {
    Iterator.continually {
      val ret = ChunkPool.allocate()
      val buf = ret.asByteBuffer
      val samples = distribution.sample(buf.remaining / 4)
      samples foreach buf.putInt
      ret
    }
  }

  def commons(distribution: IntegerDistribution): Source[Chunk, NotUsed] =
    Source.fromIterator(() => commonsIterator(distribution))

  def commons(distribution: IntegerDistribution, size: Long): Source[Chunk, NotUsed] = {
    println("invoked")
    Source.fromIterator(() => cutIterator(commonsIterator(distribution), size))
  }

  def uniform(lower: Int, upper: Int) = commons(new UniformIntegerDistribution(lower, upper))

  def uniform(lower: Int, upper: Int, size: Long) = commons(new UniformIntegerDistribution(lower, upper), size)

  def poisson(p: Double) = commons(new PoissonDistribution(p))

  def poisson(p: Double, size: Long) = commons(new PoissonDistribution(p), size)

  def zipf(n: Int, s: Double) = commons(new ZipfDistribution(n, s))

  def zipf(n: Int, s: Double, size: Long) = commons(new ZipfDistribution(n, s), size)

  private def cutIterator(iterator: Iterator[Chunk], size: Long) =
    iterator.take((size / Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize).toInt)

}
