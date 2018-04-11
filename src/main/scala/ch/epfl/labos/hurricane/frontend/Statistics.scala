package ch.epfl.labos.hurricane.frontend

import akka.agent._
import ch.epfl.labos.hurricane.Config

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

object Statistics {

  implicit var dispatcher: ExecutionContext = null
  lazy val startTime = Agent(0L)
  lazy val endTime = Agent(0L)
  lazy val ioTime = Agent(0L)
  lazy val chunksRead = Agent(0L)
  lazy val chunksWritten = Agent(0L)
  private val time2s = 0.001
  private val duration2s = 0.000000001

  def init(dispatcher_ : ExecutionContext): Unit =
    dispatcher = dispatcher_

  def runtimeF = for {
    startTime <- startTime.future
    endTime <- endTime.future
  } yield {
    (endTime - startTime) * time2s / duration2s
  }

  def totalChunksF = for {
    chunksRead <- chunksRead.future
    chunksWritten <- chunksWritten.future
  } yield {
    chunksRead + chunksWritten
  }

  def miBReadF = for {
    chunksRead <- chunksRead.future
  } yield {
    chunksRead * Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeMiB
  }

  def miBWrittenF = for {
    chunksWritten <- chunksWritten.future
  } yield {
    chunksWritten * Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeMiB
  }

  def totalIoF = for {
    totalChunks <- totalChunksF
  } yield {
    totalChunks * Config.HurricaneConfig.BackendConfig.DataConfig.chunkSizeMiB
  }

  def averageBandwidthF = for {
    totalIo <- totalIoF
    runtime <- runtimeF
  } yield {
    totalIo / (runtime * duration2s)
  }

  def ioBandwidthF = for {
    totalIo <- totalIoF
    ioTime <- ioTime.future
  } yield {
    totalIo / (ioTime * duration2s)
  }

  def statsStringF: Future[String] =
    for {
      runtime <- runtimeF
      totalIo <- totalIoF
      miBRead <- miBReadF
      miBWritten <- miBWrittenF
      averageBandwidth <- averageBandwidthF
      ioBandwidth <- ioBandwidthF
    } yield {
      s"""| ** HURRICANE STATS **
          | - Started at ${startTime.get}, ended at ${endTime.get}
          | - Runtime: ${runtime * duration2s} seconds
          | - Total I/O: ${totalIo} MB (${miBRead} MB read and ${miBWritten} MB written)
          | - Average I/O bandwidth: ${averageBandwidth} MB/s
          | - I/O bandwidth: ${ioBandwidth} MB/s
          | ** END STATS **""".stripMargin
    }

  def statsString: String =
    Await.result(statsStringF, 300 seconds)

  // Control

  def start() = startTime send System.currentTimeMillis

  def stop() = endTime send System.currentTimeMillis

  def reset() = {
    startTime send 0L
    endTime send 0L
    ioTime send 0L
    chunksRead send 0L
    chunksWritten send 0L
  }

}
