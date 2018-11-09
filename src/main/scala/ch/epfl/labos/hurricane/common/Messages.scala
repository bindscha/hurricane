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

import ch.epfl.labos.hurricane.app.Blueprint

sealed trait HurricaneMessage

sealed trait Command extends HurricaneMessage {
  def bag: Bag
}

case class Create(fingerprint: FingerPrint, bag: Bag) extends Command

case class Fill(fingerprint: FingerPrint, bag: Bag, count: Int = 4 * 1024 * 1024) extends Command

case class SeekAndFill(fingerprint: FingerPrint, bag: Bag, offset: Int, count: Int = 4 * 1024 * 1024) extends Command

case class Drain(fingerprint: FingerPrint, bag: Bag, chunk: Chunk) extends Command

case class Rewind(fingerprint: FingerPrint, bag: Bag) extends Command

case class Trunc(fingerprint: FingerPrint, bag: Bag) extends Command

case class Flush(fingerprint: FingerPrint, bag: Bag) extends Command

case class Progress(fingerprint: FingerPrint, bag: Bag) extends Command

case class Replay(oldWorker: FingerPrint, newWorker: FingerPrint, bag: Bag) extends Command

case class WorkBagUpdate(fingerprint: FingerPrint, bag: Bag, ready: List[(FingerPrint,String)], running: List[(FingerPrint,String)], done: List[(FingerPrint,String)]) extends Command

sealed trait Response extends HurricaneMessage

case object Ack extends Response

case object Nack extends Response

case object EOF extends Response

case class Filled(chunk: Chunk) extends Response

case class ProgressReport(done: Double, size: Long) extends Response

sealed trait MasterMessage

sealed trait MasterCommand extends MasterMessage

sealed trait MasterResponse extends MasterMessage

case object GetTask extends MasterCommand

case class Task(id: String, blueprint: Blueprint) extends MasterResponse

case class TaskCompleted(id: String) extends MasterCommand

case class PleaseClone(id: String) extends MasterCommand

