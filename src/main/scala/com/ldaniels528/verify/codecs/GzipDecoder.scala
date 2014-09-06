package com.ldaniels528.verify.codecs

import com.ldaniels528.verify.util.GzipCompression

import scala.util.Try

/**
 * GZIP Message Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class GzipDecoder() extends MessageDecoder[Array[Byte]] with GzipCompression {

  /**
   * Decompresses the compressed binary message
   */
  override def decode(compressedMessage: Array[Byte]): Try[Array[Byte]] = decompress(compressedMessage)

}
