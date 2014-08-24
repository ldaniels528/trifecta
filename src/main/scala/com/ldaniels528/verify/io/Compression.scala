package com.ldaniels528.verify.io

import com.ldaniels528.verify.util.VxUtils._

/**
 * Compression/Decompression Capability
 * @author lawrence.daniels@gmail.com
 */
trait Compression {
  import java.io._
  import java.util.zip._
  import scala.util.{ Try, Success, Failure }
  
  /**
   * Compresses the message 
   * @param message the given message
   * @return the compressed copy of the input message
   * @throws IOException
   */
  def compress(message: String): Array[Byte] = compress(message.getBytes("UTF-8"))

  /**
   * Compresses the message data
   * @param data the given message data
   * @return the compressed copy of the input data
   * @throws IOException
   */
  def compress(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream(data.length)
    new GZIPOutputStream(baos) { `def`.setLevel(Deflater.BEST_COMPRESSION) } use { gzos =>
      gzos.write(data)
      gzos.finish()
      baos.toByteArray()
    }
  }
  
  /**
   * Attempts to decompress the message however, if the decompression fails
   * the original message will be returned.
   */
  def deflate(message: Array[Byte]): Array[Byte] = {
    Try(decompress(message)) match {
      case Success(decompressed) => decompressed
      case Failure(e) => message
    }
  }

  /**
   * Decompresses the message data
   * @param data the given message data
   * @return the decompressed copy of the input data
   * @throws IOException
   */
  def decompress(compressedData: Array[Byte]): Array[Byte] = {
    val bais = new ByteArrayInputStream(compressedData)
    val baos = new ByteArrayOutputStream(3 * compressedData.length)
    val buf = new Array[Byte](1024)
    new GZIPInputStream(bais) use { gzin =>
      var count = 0
      do {
        count = gzin.read(buf)
        if (count > 0) {
          baos.write(buf, 0, count)
        }
      } while (count != -1)

      baos.toByteArray()
    }
  }

}