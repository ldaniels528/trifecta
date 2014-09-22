package com.ldaniels528.trifecta.support.gzip

import com.ldaniels528.trifecta.support.messaging.MessageDecoder

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
