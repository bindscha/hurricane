package ch.epfl.labos.hurricane.common

import ch.epfl.labos.hurricane.app.Blueprint

sealed trait HurricaneMessage

sealed trait Command extends HurricaneMessage {
  def bag: Bag
  def fingerprint: FingerPrint
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
