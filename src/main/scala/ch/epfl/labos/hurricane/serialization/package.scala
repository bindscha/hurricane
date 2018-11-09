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
package ch.epfl.labos.hurricane

package object serialization {

  import java.nio.ByteBuffer

  implicit class StringRichByteBuffer(val buf: ByteBuffer) extends AnyVal with RichByteBuffer[String] {
    def serializeTo(str: String): Unit = buf.put(str.getBytes)
    def deserializeFrom(): String = null // implicit deserialize does not work for String
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
