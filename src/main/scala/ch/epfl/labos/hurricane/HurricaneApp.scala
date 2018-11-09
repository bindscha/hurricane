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
package ch.epfl.labos.hurricane

import ch.epfl.labos.hurricane.backend.HurricaneBackend
import ch.epfl.labos.hurricane.frontend.HurricaneFrontend

object HurricaneApp {

  def main(args: Array[String]): Unit = {
    Config.ModeConfig.mode match {
      case Config.ModeConfig.Dev =>
        // Testing: starting 3 backend nodes and 1 frontend node
        HurricaneBackend.main(Seq("--me", "0").toArray)
        HurricaneBackend.main(Seq("--me", "1").toArray)
        HurricaneBackend.main(Seq("--me", "2").toArray)
        HurricaneFrontend.main(args)
        // Warning: do not start more frontends because Config is a shared singleton object!
      case Config.ModeConfig.Prod =>
        // Prod: starting 1 backend node and 1 frontend node
        HurricaneBackend.main(Array.empty)
        Thread.sleep(5000) // sleep 2 seconds to make sure the backend is started
        HurricaneFrontend.main(args)
    }
  }

}