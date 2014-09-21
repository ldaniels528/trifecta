package com.ldaniels528.verify.util

import com.ldaniels528.verify.VxConfig

/**
 * Provides implementing classes with the capability of displaying binary messages
 */
trait BinaryMessaging {

  /**
   * Displays the contents of the given message
   * @param offset the offset of the given message
   * @param message the given message
   * @return the size of the message in bytes
   */
  def dumpMessage(offset: Long, message: Array[Byte])(implicit config: VxConfig) {
    // determine the widths for each section: bytes & characters
    val columns = config.columns
    val byteWidth = config.columns * 3
    val charWidth = config.columns + 1

    // display the message
    var index = 0
    val length1 = Math.max(4, 1 + Math.log10(offset).toInt)
    val length2 = Math.max(3, 1 + Math.log10(message.length).toInt)
    val myFormat = s"[%0${length1}d:%0${length2}d] %-${byteWidth}s| %-${charWidth}s|"
    message.sliding(columns, columns) foreach { bytes =>
      config.out.println(myFormat.format(offset, index, asHexString(bytes), asChars(bytes)))
      index += columns
    }
  }

  /**
   * Displays the contents of the given message
   * @param message the given message
   * @return the size of the message in bytes
   */
  def dumpMessage(message: Array[Byte])(implicit config: VxConfig) {
    // determine the widths for each section: bytes & characters
    val columns = config.columns
    val byteWidth = config.columns * 3
    val charWidth = config.columns + 1

    // display the message
    var offset = 0
    val length = Math.max(3, 1 + Math.log10(message.length).toInt)
    val myFormat = s"[%0${length}d] %-${byteWidth}s| %-${charWidth}s|"
    message.sliding(columns, columns) foreach { bytes =>
      config.out.println(myFormat.format(offset, asHexString(bytes), asChars(bytes)))
      offset += columns
    }
  }

  /**
   * Returns the ASCII array as a character string
   * @param bytes the byte array
   * @return a character string representing the given byte array
   */
  protected def asChars(bytes: Array[Byte]): String = {
    String.valueOf(bytes map (b => if (b >= 32 && b <= 126) b.toChar else '.'))
  }

  /**
   * Returns the byte array as a hex string
   * @param bytes the byte array
   * @return a hex string representing the given byte array
   */
  protected def asHexString(bytes: Array[Byte]): String = {
    bytes map ("%02x".format(_)) mkString "."
  }

}
