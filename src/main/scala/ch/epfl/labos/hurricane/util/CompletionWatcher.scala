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

import akka._
import akka.actor._

import scala.collection._
import scala.concurrent._

case class Completed(id: String)

object CompletionWatcher {

  def watch()(implicit dispatcher: ExecutionContext): (Promise[Done], Future[Done]) = {
    val promise = Promise[Done]()
    val future = promise.future
    (promise, future)
  }

  def watch(n: Int)(implicit dispatcher: ExecutionContext): (Seq[Promise[Done]], Future[Done]) = {
    val promises = (0 until n) map (_ => Promise[Done]())
    val tmp = Future.foldLeft(promises.map(_.future))(Done)((a,_) => a)
    (promises, tmp)
  }

}

class CompletionWatcher(ids: Iterable[String], promise: Promise[Done]) extends Actor {

  val toWatch = mutable.Set.empty[String]

  override def preStart(): Unit = {
    super.preStart()

    ids foreach ( toWatch += _ )
  }

  override def postStop(): Unit = {
    promise.success(Done)

    super.postStop()
  }

  override def receive = {
    case Completed(id) =>
      toWatch -= id
      if(toWatch.isEmpty) {
        context stop self
      }
  }
}