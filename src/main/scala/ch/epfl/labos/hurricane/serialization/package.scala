package ch.epfl.labos.hurricane

package object serialization {

  import java.nio.ByteBuffer

  implicit class StringRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[String] {
    def serializeTo(str: String): Unit = buf.put(str.getBytes)
    def deserializeFrom(): String = null // XXX: crap, that doesn't work for strings
  }

  implicit class IntRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[Int] {
    def serializeTo(i: Int): Unit = buf.putInt(i)
    def deserializeFrom(): Int = buf.getInt
  }

  implicit class LongRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[Long] {
    def serializeTo(l: Long): Unit = buf.putLong(l)
    def deserializeFrom(): Long = buf.getLong
  }

  implicit class FloatRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[Float] {
    def serializeTo(f: Float): Unit = buf.putFloat(f)
    def deserializeFrom(): Float = buf.getFloat
  }

  implicit class DoubleRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[Double] {
    def serializeTo(d: Double): Unit = buf.putDouble(d)
    def deserializeFrom(): Double = buf.getDouble
  }


  implicit object IntFormat extends PositionalFormat[Int] with IntSerializer
  implicit object LongFormat extends PositionalFormat[Long] with LongSerializer
  implicit object FloatFormat extends PositionalFormat[Float] with FloatSerializer
  implicit object DoubleFormat extends PositionalFormat[Double] with DoubleSerializer
  implicit object CharFormat extends PositionalFormat[Char] with CharSerializer
  implicit object ByteFormat extends PositionalFormat[Byte] with ByteSerializer
  implicit object ShortFormat extends PositionalFormat[Short] with ShortSerializer
  implicit object IntIntFormat extends LittleEndianPositionalFormat[(Int,Int)] with IntIntSerializer
  implicit object SpacedStringFormat extends StringFormat(' ')

  object LineByLineStringFormat extends StringFormat('\n')

}
