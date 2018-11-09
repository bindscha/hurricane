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

import java.io._
import java.net.URI
import java.util.EnumSet

import akka.Done
import akka.actor._
import akka.stream.actor._
import akka.util.Timeout
import ch.epfl.labos.hurricane.Config
import ch.epfl.labos.hurricane.Config.HurricaneConfig
import ch.epfl.labos.hurricane.common._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileContext, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Metadata}
import org.apache.hadoop.io.{ByteWritable, IntWritable, SequenceFile}

import scala.collection.immutable.Map
import scala.concurrent.{Future, Promise}

object Hdfs extends App {

  def buildHadoopConfig() = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName())
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName())
    conf
  }

  def openTextFile(uri: String, filePath: String) = {

    System.setProperty("HADOOP_HOME", HurricaneConfig.HdfsConfig.hadoop_home)
    System.setProperty("hadoop.home.dir", HurricaneConfig.HdfsConfig.hadoop_home)
    val conf = buildHadoopConfig()

    val hdfs: FileSystem = FileSystem.get(new URI("hdfs://" + uri), conf)
    val fileName: Path = new Path ("hdfs://" + uri + filePath)

    hdfs.open(fileName)
  }

  def createTextFile(uri: String, filePath: String) = {
    System.setProperty("HADOOP_HOME", HurricaneConfig.HdfsConfig.hadoop_home)
    System.setProperty("hadoop.home.dir", HurricaneConfig.HdfsConfig.hadoop_home)
    val conf = buildHadoopConfig()

    val hdfs: FileSystem = FileSystem.get(new URI("hdfs://" + uri), conf)
    val fileName: Path = new Path ("hdfs://" + uri + filePath)

    hdfs.create(fileName, HurricaneConfig.HdfsConfig.replication_factor)
  }

  def textReader(uri: String, filePath: String) =
    new BufferedReader(new InputStreamReader(openTextFile(uri, filePath), "UTF-8"))

  def textWriter(uri: String, filePath: String) = {
    new BufferedWriter(new OutputStreamWriter(createTextFile(uri, filePath), "UTF-8"))
  }

  def writer(uri: String, filePath: String) = {
    System.setProperty("HADOOP_HOME", HurricaneConfig.HdfsConfig.hadoop_home)
    System.setProperty("hadoop.home.dir", HurricaneConfig.HdfsConfig.hadoop_home)
   
    val fileContext: FileContext = FileContext.getFileContext(new URI("hdfs://" + uri))	
    val path = new Path("hdfs://"+ uri + filePath)
    val conf = buildHadoopConfig()

    val metadata: Metadata = new Metadata()
    val createFlags = EnumSet.of(CreateFlag.CREATE,CreateFlag.APPEND)
  
	
    val createOpts = {
      CreateOpts.repFac(HurricaneConfig.HdfsConfig.replication_factor)
    }

    SequenceFile.createWriter(fileContext, conf, path, classOf[ByteWritable], classOf[IntWritable], CompressionType.NONE, null, metadata, createFlags, createOpts)
  }

}

object HdfsFiller {
  def props(bag: Bag, completionPromise: Option[Promise[Done]] = None): Props = Props(classOf[HdfsFiller], bag.id, completionPromise) // Unpacking manually because bag is value class

  implicit val timeout = Timeout(Config.HurricaneConfig.FrontendConfig.sourceRetry)
}

class HdfsFiller(uri: String, file: String, completionPromise: Option[Promise[Done]] = None) extends ActorPublisher[Chunk] with ActorLogging with ActorCounter {

  val handle = Hdfs.openTextFile(uri, file)

  override def preStart(): Unit = {
    log.info("Started HDFS filler for file " + file)
    deliver()

    super.preStart()
  }

  override def postStop(): Unit = {
    log.info("Stopped HDFS filler for file " + file)

    super.postStop()
  }

  def close() = {
    completionPromise foreach { _.success(Done) }
    context stop self
  }

  override def receive = {
    case ActorPublisherMessage.Request(_) =>
      deliver()

    case ActorPublisherMessage.Cancel =>
      deliver()
      close()
  }

  def deliver(): Unit = {
    if (totalDemand > 0) {
      val chunk = ChunkPool.allocate()
      handle.read(chunk.asByteBuffer)

      onNext(chunk)
    }
  }

}

object HdfsDrainer {

  def props(uri: String, file: String, completionPromise: Option[Promise[Done]] = None): Props = Props(classOf[HdfsDrainer], uri, file, completionPromise)

  implicit val timeout = Timeout(Config.HurricaneConfig.FrontendConfig.sinkRetry)

  case class Acked(id: ActorCounter.Id)
  case object Finish

}

class HdfsDrainer(uri: String, file: String, completionPromise: Option[Promise[Done]] = None) extends ActorSubscriber with ActorLogging with ActorCounter {

  import HdfsDrainer._
  import context.dispatcher

  var queue = Map.empty[ActorCounter.Id, Chunk]
  val handle = new OutputStreamWriter(Hdfs.createTextFile(uri, file), "UTF-8") //writer(uri, file) - Binary writer

  override val requestStrategy =
    new MaxInFlightRequestStrategy(max = Config.HurricaneConfig.FrontendConfig.sinkQueueSize) {
      override def inFlightInternally: Int = queue.size
    }

  override def preStart(): Unit = {
    super.preStart()

    log.info("Started HDFS drainer for file " + file)
  }

  override def postStop(): Unit = {
    log.info("Stopped HDFS drainer for file " + file)

    super.postStop()
  }

  def close() = {
    log.info(s"Flushing file $file")
    println(" *************** REPLICATION " + HurricaneConfig.HdfsConfig.replication_factor)
    try {
      handle.close()
    } catch {
      case _: Throwable => // ignore
    }

    completionPromise foreach {
      _.success(Done)
    }

    context stop self
  }

  override def receive = {
    case Acked(id) =>
      queue -= id
      if (canceled && queue.isEmpty) {
        close()
      }

    case ActorSubscriberMessage.OnNext(chunk: Chunk) =>
      val id = nextId
      queue += id -> chunk
      drainChunk(id, chunk)

    case ActorSubscriberMessage.OnComplete =>
      if (queue.isEmpty) {
        close()
      }
  }

  def drainChunk(id: ActorCounter.Id, chunk: Chunk): Unit = {

    val me = self
    val tmpHandle = handle
    def innerDrainChunk(): Future[_] = Future {
      val data = new String(chunk.array).substring(0, Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize).trim + '\n'
      tmpHandle.write(data)
      me ! Acked(id)
    }

    innerDrainChunk() recoverWith { case _: Throwable => innerDrainChunk() }

  }

}
