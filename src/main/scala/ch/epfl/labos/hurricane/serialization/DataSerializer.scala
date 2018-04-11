package ch.epfl.labos.hurricane.serialization

import java.nio._

import ch.epfl.labos.hurricane.util._

trait DataSerializer[A] {

  def toBinary(buf: ByteBuffer, something: A): Unit
  def fromBinary(buf: ByteBuffer): A

}

trait IntSerializer extends DataSerializer[Int] {

  override def toBinary(buf: ByteBuffer, i: Int): Unit = buf.putInt(i)
  override def fromBinary(buf: ByteBuffer): Int = buf.getInt()

}

trait LongSerializer extends DataSerializer[Long] {

  override def toBinary(buf: ByteBuffer, l: Long): Unit = buf.putLong(l)
  override def fromBinary(buf: ByteBuffer): Long = buf.getLong()

}

trait FloatSerializer extends DataSerializer[Float] {

  override def toBinary(buf: ByteBuffer, f: Float): Unit = buf.putFloat(f)
  override def fromBinary(buf: ByteBuffer): Float = buf.getFloat()

}

trait DoubleSerializer extends DataSerializer[Double] {

  override def toBinary(buf: ByteBuffer, d: Double): Unit = buf.putDouble(d)
  override def fromBinary(buf: ByteBuffer): Double = buf.getDouble()

}

trait CharSerializer extends DataSerializer[Char] {

  override def toBinary(buf: ByteBuffer, c: Char): Unit = buf.putChar(c)
  override def fromBinary(buf: ByteBuffer): Char = buf.getChar()

}

trait ByteSerializer extends DataSerializer[Byte] {

  override def toBinary(buf: ByteBuffer, b: Byte): Unit = buf.put(b)
  override def fromBinary(buf: ByteBuffer): Byte = buf.get()

}

trait ShortSerializer extends DataSerializer[Short] {

  override def toBinary(buf: ByteBuffer, s: Short): Unit = buf.putShort(s)
  override def fromBinary(buf: ByteBuffer): Short = buf.getShort()

}

trait IntIntSerializer extends DataSerializer[(Int,Int)] {

  override def toBinary(buf: ByteBuffer, ij: (Int,Int)): Unit = { buf.putInt(ij._1); buf.putInt(ij._2) }
  override def fromBinary(buf: ByteBuffer): (Int,Int) = (buf.getInt(), buf.getInt())
}

trait StringSerializer extends DataSerializer[String] {

  // Careful: deserializing string from byte buffer will read a string with all remaining bytes

  override def toBinary(buf: ByteBuffer, str: String): Unit = buf.put(str.getBytes)
  override def fromBinary(buf: ByteBuffer): String = {
    val ret = new Array[Byte](buf.remaining)
    buf.get(ret)
    new String(ret)
  }

}

trait Format[A] {
  this: DataSerializer[A] =>

  def reads(buf: ByteBuffer): A = fromBinary(buf)
  def writes(buf: ByteBuffer, something: A): Unit = toBinary(buf, something)

  def iterator(buf: ByteBuffer): Iterator[A]
  def pusher(buf: ByteBuffer): Pusher[A]

}

trait PositionalFormat[A] extends Format[A] {
  this: DataSerializer[A] =>

  def byteOrder: ByteOrder = ByteOrder.BIG_ENDIAN

  override def iterator(buf: ByteBuffer): Iterator[A] = new Iterator[A] {
    buf.order(byteOrder)
    override def hasNext: Boolean = buf.hasRemaining
    override def next(): A = fromBinary(buf)
  }

  override def pusher(buf: ByteBuffer): Pusher[A] = new Pusher[A] {
    buf.order(byteOrder)
    override def pushed: Int = buf.position
    override def hasRemaining: Boolean = buf.hasRemaining
    override def put(something: A): Int =
      try {
        toBinary(buf, something)
        0
      } catch {
        case e: BufferOverflowException => -1
      }
  }

}

trait LittleEndianPositionalFormat[A] extends PositionalFormat[A] {
  this: DataSerializer[A] =>

  override def byteOrder = ByteOrder.LITTLE_ENDIAN
}

trait DataSerializers[A,B] extends DataSerializer[A] {
  def separatorSerializer: DataSerializer[B]
}

trait SeparatorFormat[A,B] extends Format[A] {
  this: DataSerializer[A] =>

  def separator: B
  def separatorSerializer: DataSerializer[B]

  override def iterator(buf: ByteBuffer): Iterator[A] =
    throw new UnsupportedOperationException("Iterator not implemented!") // XXX: can't think of a clean/easy way to do this generically

  override def pusher(buf: ByteBuffer): Pusher[A] = new Pusher[A] { // XXX: this one is not safe because it might omit the separator
    override def pushed: Int = buf.position
    override def hasRemaining: Boolean = buf.hasRemaining
    override def put(something: A): Int =
      try {
        toBinary(buf, something)
        separatorSerializer.toBinary(buf, separator)
        0
      } catch {
        case e: BufferOverflowException =>
          -1
      }
  }

}

class StringFormat(override val separator: Char) extends SeparatorFormat[String,Char] with StringSerializer {

  override val separatorSerializer = CharFormat

  override def iterator(buf: ByteBuffer): Iterator[String] = StringUtils.tokenize(fromBinary(buf), separator)

  override def pusher(buf: ByteBuffer): Pusher[String] = new Pusher[String] {
    override def pushed: Int = buf.position
    override def hasRemaining: Boolean = buf.hasRemaining
    override def put(something: String): Int = try {
      toBinary(buf, something + separator)
      0
    } catch {
      case e: BufferOverflowException =>
        -1
    }
  }

}


