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
package ch.epfl.labos.hurricane.app

import ch.epfl.labos.hurricane.common.Bag

import scala.collection.immutable.Seq

object Blueprint {
  def apply(name: String, appConf: AppConf, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq.empty, Seq.empty, needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), Seq.empty, needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, output: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), Seq(output), needsMerge)
  def apply(name: String, appConf: AppConf, inputs: Seq[Bag], output: Bag, needsMerge: Boolean): Blueprint = Blueprint(name, appConf, inputs, Seq(output), needsMerge)
  def apply(name: String, appConf: AppConf, input: Bag, outputs: Seq[Bag], needsMerge: Boolean): Blueprint = Blueprint(name, appConf, Seq(input), outputs, needsMerge)
}

case class Blueprint(name: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag], needsMerge: Boolean)
