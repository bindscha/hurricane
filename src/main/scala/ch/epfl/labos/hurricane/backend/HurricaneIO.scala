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
package ch.epfl.labos.hurricane.backend

import java.io.{File => _, _}

import akka.actor._
import better.files.File
import ch.epfl.labos.hurricane._
import ch.epfl.labos.hurricane.common._
import ch.epfl.labos.hurricane.frontend.Statistics
import net.smacke.jaydio._

object HurricaneIO {

  def props(bag: Bag, root: File): Props =
    Config.HurricaneConfig.BackendConfig.DataConfig.ioEngine match {
      case Config.HurricaneConfig.BackendConfig.DataConfig.DefaultIOEngine =>
        Props(classOf[HurricaneIO], bag.id, root).withDispatcher("hurricane.backend.blocking-io-dispatcher")
      case Config.HurricaneConfig.BackendConfig.DataConfig.DirectIOEngine =>
        Props(classOf[HurricaneDIO], bag.id, root).withDispatcher("hurricane.backend.blocking-io-dispatcher")
    }

  sealed trait Operation

  case object Read extends Operation

  case object Write extends Operation

  case object Noop extends Operation

  /*def main(args: Array[String]): Unit = {
    println("Hurricane IO test")

    val size = 10L * 1024 * 1024 * 1024
    val chunk = 4 * 1024 * 1024
    val f = new RandomAccessFile(File("test").toJava, "rw")

    val serializer = new ch.epfl.labos.hurricane.serialization.WireSerializer

    val buf = ByteBuffer.allocate(2 * chunk)

    val start = System.nanoTime
    for(i <- 0 until (size / chunk).toInt) {
      println(i)
      val buffer = new Array[Byte](chunk)
      val drain = Drain(Bag.random, buffer)
      serializer.toBinary(drain, buf)
      buf.rewind()
      serializer.fromBinary(buf, "")
      buf.rewind()
      val array = new Array[Byte](buf.remaining)
      serializer.toBinary(drain, buf)
      f.write(array)
    }
    f.close()
    val stop = System.nanoTime

    val runtime = (stop - start) * 0.000000001

    println("Runtime:" + runtime + " seconds")
    println("Bandwidth: " + (size * 0.000001) / runtime + " MB/s")
  }*/
}

class HurricaneIO(bag: Bag, root: File = File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HurricaneIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new RandomAccessFile((root / bag.id).toJava, "rw")

  def receive = {
    case Create(fingerprint, file) =>
      // do nothing
    case Fill(fingerprint, file, count) =>
      val buffer = ChunkPool.allocate()
      val read = withStats(Read) {
        inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
      }
      if(read >= 0) {
        buffer.chunkSize(read)
        sender ! Filled(buffer)
      } else {
        sender ! EOF
      }
    case SeekAndFill(fingerprint, file, offset, count) =>
      val buffer = ChunkPool.allocate()
      val fp = inout.getFilePointer
      val read = withStats(Read) {
        inout.seek(offset)
        val ret = inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        inout.seek(fp)
        ret
      }
      if(read >= 0) {
        buffer.chunkSize(read)
        sender ! Filled(buffer)
      } else {
        sender ! EOF
      }
    case Drain(fingerprint, file, data) =>
      withStats(Write) {
        inout.write(data.array, 0, data.chunkSize)
      }
      sender ! Ack
    case Rewind(fingerprint, file) =>
      withStats(Noop) {
        inout.seek(0L)
      }
      sender ! Ack
    case Trunc(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
        inout = new RandomAccessFile((root / bag.id).toJava, "rw")
      }
      sender ! Ack
    case Flush(fingerprint, file) =>
      withStats(Noop) {
        inout.getFD.sync()
      }
      sender ! Ack
    case Progress(fingerprint, file) =>
      val done = if(inout.length == 0) 1.0 else inout.getFilePointer.toDouble / inout.length.toDouble
      sender ! ProgressReport(done, inout.length)
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}

// This is for benchmarking/testing only. Not tested properly!!
class HurricaneDIO(bag: Bag, root: File = File(Config.HurricaneConfig.BackendConfig.DataConfig.dataDirectory.getAbsolutePath)) extends Actor with ActorLogging {

  import HurricaneIO._

  // Create root if not exists
  root.createDirectories()

  private var inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)

  def receive = {
    case Create(fingerprint, file) =>
    // do nothing
    case Fill(fingerprint, file, count) =>
      val buffer = ChunkPool.allocate()
      withStats(Read) {
        inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        buffer.array.length
      }
      sender ! Filled(buffer)
    case SeekAndFill(fingerprint, file, offset, count) =>
      val buffer = ChunkPool.allocate()
      val fp = inout.getFilePointer
      withStats(Read) {
        inout.seek(offset)
        inout.read(buffer.array, 0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize)
        inout.seek(fp)
        buffer.array.length
      }
      sender ! Filled(buffer)
    case Drain(fingerprint, file, data) =>
      withStats(Write) {
        inout.write(data.array, 0, data.chunkSize)
        Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize
      }
      sender ! Ack
    case Rewind(fingerprint, file) =>
      withStats(Noop) {
        inout.seek(0L)
      }
      sender ! Ack
    case Trunc(fingerprint, file) =>
      withStats(Noop) {
        inout.close()
        (root / bag.id).delete(true)
        inout = new DirectRandomAccessFile((root / bag.id).toJava, "rw", 4 * 1024 * 1024)
      }
      sender ! Ack
    case Flush(fingerprint, file) => // no need to flush, but we still ack it
      sender ! Ack
    case Progress(fingerprint, file) =>
      val done = if(inout.length == 0) 1.0 else inout.getFilePointer.toDouble / inout.length.toDouble
      sender ! ProgressReport(done, inout.length)
  }

  def withStats[A](op: Operation)(f: => A): A = {
    val started = System.nanoTime()
    val ret = f
    Statistics.ioTime send (_ + (System.nanoTime - started))
    op match {
      case Read if ret.asInstanceOf[Int] > 0 => Statistics.chunksRead send (_ + 1)
      case Write => Statistics.chunksWritten send (_ + 1)
      case _ =>
    }
    ret
  }

}
