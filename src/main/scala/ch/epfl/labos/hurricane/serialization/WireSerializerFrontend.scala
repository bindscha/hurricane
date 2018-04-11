package ch.epfl.labos.hurricane.serialization

import akka.serialization._
import ch.epfl.labos.hurricane.frontend.{Ready, Synced}

object WireSerializerFrontend {
  val COMMAND_LENGTH = 4

  val READY_COMMAND = "REDY"
  val SYNCED_COMMAND = "TOKN"
}

class WireSerializerFrontend extends Serializer {

  import WireSerializerFrontend._

  def includeManifest: Boolean = false

  def identifier = 31415927

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case Ready(id) =>
      READY_COMMAND.getBytes ++ id.toString.getBytes
    case Synced =>
      SYNCED_COMMAND.getBytes
  }

  def fromBinary(
                  bytes: Array[Byte],
                  clazz: Option[Class[_]]): AnyRef = {
    bytes.slice(0, COMMAND_LENGTH)
    new String(bytes.take(COMMAND_LENGTH)) match {
      case READY_COMMAND => Ready(new String(bytes.drop(COMMAND_LENGTH)).toInt)
      case SYNCED_COMMAND => Synced
    }
  }
}

