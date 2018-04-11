package ch.epfl.labos.hurricane.frontend

trait CloningStrategy {

  def offer(cpuLoad: Double, networkLoad: Double, progress: Double): Boolean

}

case class ProgressBasedCloningStrategy(maxProgress: Double) extends CloningStrategy {

  override def offer(cpuLoad: Double, networkLoad: Double, progress: Double): Boolean = progress <= maxProgress

}
