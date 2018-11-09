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
package ch.epfl.labos.hurricane.serialization

object WireSerializerControl {
  val COMMAND_LENGTH = 4

  val GETTASK_COMMAND = "GETT"
  val TASK_COMMAND = "TASK"
  val TASKCOMPLETED_COMMAND = "TDON"
}

/*class WireSerializerControl extends Serializer {

  import WireSerializerControl._

  val serializer = SerializationExtension(system).findSerializerFor(original)

  def includeManifest: Boolean = false

  def identifier = 31415928

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case GetTask =>
      GETTASK_COMMAND.getBytes
    case TaskCompleted(id) =>
      TASKCOMPLETED_COMMAND.getBytes ++ id.toString.getBytes
  }

  def fromBinary(
                  bytes: Array[Byte],
                  clazz: Option[Class[_]]): AnyRef = {
    bytes.slice(0, COMMAND_LENGTH)
    new String(bytes.take(COMMAND_LENGTH)) match {
      case GETTASK_COMMAND => GetTask
      case TASK_COMMAND => Task(new String(bytes.drop(COMMAND_LENGTH)))
      case TASKCOMPLETED_COMMAND => TaskCompleted(new String(bytes.drop(COMMAND_LENGTH)))
    }
  }
}*/

