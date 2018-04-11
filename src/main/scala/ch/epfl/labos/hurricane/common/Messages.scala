package ch.epfl.labos.hurricane.common

import ch.epfl.labos.hurricane.app.Blueprint

sealed trait HurricaneMessage

sealed trait Command extends HurricaneMessage {
  def bag: Bag
}

case class Create(bag: Bag) extends Command

case class Fill(bag: Bag, count: Int = 4 * 1024 * 1024) extends Command

case class SeekAndFill(bag: Bag, offset: Int, count: Int = 4 * 1024 * 1024) extends Command

case class Drain(bag: Bag, chunk: Chunk) extends Command

case class Rewind(bag: Bag) extends Command

case class Trunc(bag: Bag) extends Command

case class Flush(bag: Bag) extends Command

case class Progress(bag: Bag) extends Command

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
