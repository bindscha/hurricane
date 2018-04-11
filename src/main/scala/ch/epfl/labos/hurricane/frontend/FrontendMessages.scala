package ch.epfl.labos.hurricane.frontend

sealed trait FrontendMessage

case class Ready(id: Int) extends FrontendMessage

case object Synced extends FrontendMessage
