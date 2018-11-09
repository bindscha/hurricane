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

object StringUtils {

  def tokenize(str: String, token: Char): Iterator[String] = new Iterator[String] {
    var i = 0

    override def hasNext: Boolean = i < str.length

    override def next(): String = {
      val j = str.indexOf(token, i)
      if(j >= 0) {
        val ret = str.substring(i, j)
        i = j + 1
        ret
      } else {
        val ret = str.substring(i)
        i = str.length
        ret
      }
    }
  }

  /*def untokenize(iter: Iterator[String], token: Char): String = {
    val buf = new StringBuilder(Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize.toInt)
    iter.foreach(str => buf.append(str + token))
    buf.result
  }*/

}
