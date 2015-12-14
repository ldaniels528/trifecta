package com.github.ldaniels528.trifecta.io.gzip

import com.github.ldaniels528.trifecta.messages.{MessageDecoder, MessageEncoder}

import scala.util.Try

/**
 * GZIP Message Codec
 * @author lawrence.daniels@gmail.com
 */
object GzipCodec extends GzipCompression with MessageDecoder[Array[Byte]] with MessageEncoder[Array[Byte]] {

  /**
   * Decompresses the compressed binary message
   * @param message the given compressed binary message
   * @return a decompressed message wrapped in a Try-monad
   */
  override def decode(message: Array[Byte]): Try[Array[Byte]] = decompress(message)

  /**
   * Compresses the given binary message
   * @param message the given binary message
   * @return a compressed message wrapped in a Try-monad
   */
  override def encode(message: Array[Byte]): Try[Array[Byte]] = compress(message)

}
