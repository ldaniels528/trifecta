package com.ldaniels528.verify.util

import java.nio.ByteBuffer

/**
 * Byte Buffer Utilities
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ByteBufferUtils {

  def toArray(payload: ByteBuffer): Array[Byte] = {
    val bytes = new Array[Byte](payload.limit)
    payload.get(bytes)
    bytes
  }

}
