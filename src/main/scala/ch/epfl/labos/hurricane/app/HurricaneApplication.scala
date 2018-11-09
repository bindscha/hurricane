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

import akka.actor.Props
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend._

import scala.collection.immutable.Seq
import scala.concurrent._

trait HurricaneApplication {

  def phases(appConf: AppConf)(implicit dispatcher: ExecutionContext): Seq[Seq[Props]] =
    blueprints(appConf).map(phase => phase.map(blueprint => instantiate(blueprint)))

  def sequentialExecution(appConf: AppConf)(implicit dispatcher: ExecutionContext): Props =
    SeqWorkExecutor.props(phases(appConf).flatMap(p => Seq(WorkExecutor.props(HurricaneFrontendSync.props(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes)), phase2props(p))) :+ WorkExecutor.props(HurricaneFrontendSync.props(Config.HurricaneConfig.FrontendConfig.NodesConfig.nodes)))

  def sequentialExecutionDescription(appConf: AppConf)(implicit dispatcher: ExecutionContext): String =
    (for {
      (phase, i) <- phases(appConf).zipWithIndex
    } yield { s"\nPhase $i:\n" + phase.map(_.args.lastOption.map("- " + _) getOrElse "- (no description)").mkString("\n") }) mkString "\n"

  private def phase2props(phase: Seq[Props]): Props = {
    filterWork(phase).toList match {
      case Nil => WorkExecutor.props(HurricaneNoop.props)
      case w :: Nil => WorkExecutor.props(w)
      case ws => SeqWorkExecutor.props(ws map WorkExecutor.props)
    }
  }

  private val m = Config.HurricaneConfig.FrontendConfig.NodesConfig.machines
  private val me = Config.HurricaneConfig.me

  protected def filterWork(seq: Seq[Props]): Seq[Props] = seq.indices.filter(_ % m == me) map seq


  // New API

  def instantiate(blueprint: Blueprint)(implicit dispatcher: ExecutionContext): Props

  def blueprints(appConf: AppConf): Seq[Seq[Blueprint]]

  def merge(phase: String, appConf: AppConf, inputs: Seq[Bag], outputs: Seq[Bag]): Option[Blueprint] = None

}
