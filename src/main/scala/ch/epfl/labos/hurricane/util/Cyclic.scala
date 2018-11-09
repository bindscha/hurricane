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

import scala.collection.immutable.Seq
import scala.util.Random

object Cyclic {

  def apply[A](seed: Int, nodes: Seq[A]): Cyclic[A] =
    new Cyclic[A](seed, nodes)

}

class Cyclic[A](seed: Int, nodes: Seq[A]) {

  val permutation = new Random(3 * seed).shuffle(new Random(seed).shuffle(nodes)) // need to shuffle twice with this RNG
  val size = permutation.size

  private var i = 0

  def next: A = {
    val ret = permutation(i)
    i = (i + 1) % size
    ret
  }

}
