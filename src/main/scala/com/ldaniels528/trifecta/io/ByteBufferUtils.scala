package com.ldaniels528.trifecta.io

import java.nio.ByteBuffer

/**
 * Byte Buffer Utilities
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ByteBufferUtils {

  def toArray(buffer: ByteBuffer): Array[Byte] = {
    (for {
      buf <- Option(buffer)
      limit = buf.limit()
    } yield {
      val bytes = new Array[Byte](limit)
      buf.get(bytes)
      bytes
    }) getOrElse Array.empty
  }

  def intToBytes(value: Int): Array[Byte] = {
    ByteBuffer.allocate(4).putInt(value).array()
  }

  def longToBytes(value: Long): Array[Byte] = {
    ByteBuffer.allocate(8).putLong(value).array()
  }

}
