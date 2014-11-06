package com.ldaniels528.trifecta.decoders

import com.ldaniels528.trifecta.support.gzip.GzipCompression
import com.ldaniels528.trifecta.support.messaging.{MessageDecoder, MessageEncoder}

import scala.util.Try

/**
 * GZIP Message Codec
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class GzipCodec() extends GzipCompression with MessageDecoder[Array[Byte]] with MessageEncoder[Array[Byte]] {

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
