package ch.epfl.labos.hurricane.serialization

object WireSerializerControl {
  val COMMAND_LENGTH = 4

  val GETTASK_COMMAND = "GETT"
  val TASK_COMMAND = "TASK"
  val TASKCOMPLETED_COMMAND = "TDON"
}

/*class WireSerializerControl extends Serializer {

  import WireSerializerControl._

  val serializer = SerializationExtension(system).findSerializerFor(original)

  def includeManifest: Boolean = false

  def identifier = 31415928

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case GetTask =>
      GETTASK_COMMAND.getBytes
    case TaskCompleted(id) =>
      TASKCOMPLETED_COMMAND.getBytes ++ id.toString.getBytes
  }

  def fromBinary(
                  bytes: Array[Byte],
                  clazz: Option[Class[_]]): AnyRef = {
    bytes.slice(0, COMMAND_LENGTH)
    new String(bytes.take(COMMAND_LENGTH)) match {
      case GETTASK_COMMAND => GetTask
      case TASK_COMMAND => Task(new String(bytes.drop(COMMAND_LENGTH)))
      case TASKCOMPLETED_COMMAND => TaskCompleted(new String(bytes.drop(COMMAND_LENGTH)))
    }
  }
}*/

