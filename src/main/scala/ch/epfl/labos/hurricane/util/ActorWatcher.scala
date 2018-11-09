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

import akka.actor._
import akka._

import scala.collection._
import scala.concurrent._

object ActorWatcher {

  def watch(refs: Iterable[ActorRef])(implicit context: ActorContext): Future[Done] =
  if (refs.isEmpty) {
    Future.successful(Done)
  } else {
    val promise = Promise[Done]()
    context.actorOf(Props(classOf[ActorWatcher], refs, promise))
    promise.future
  }

}

class ActorWatcher(refs: Iterable[ActorRef], promise: Promise[Done]) extends Actor {

  val toWatch = mutable.Set.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()

    refs foreach { ref =>
      toWatch += ref
      context watch ref
    }
  }

  override def postStop(): Unit = {
    promise.success(Done)

    super.postStop()
  }

  override def receive = {
    case Terminated(ref) =>
      toWatch -= ref
      if(toWatch.isEmpty) {
        context stop self
      }
  }
}