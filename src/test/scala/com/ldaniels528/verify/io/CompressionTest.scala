package com.ldaniels528.verify.io

import com.ldaniels528.verify.io.Compression._
import org.junit._
import org.slf4j.LoggerFactory

/**
 * Compression Test Suite
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CompressionTest {
  private val logger = LoggerFactory.getLogger(getClass)

  @Test
  def testCompression(): Unit = {
    // define the message
    val originalMessage = "This is a sample sentence to compress"
    val encoding = "UTF8"

    // compress & decompress the message
    val result = for {
      compressedData <- originalMessage.compress(encoding)
      uncompressedData <- compressedData.decompress
    } yield new String(uncompressedData, encoding)

    // compare the results
    logger.info("The message should have been compressed and decompressed without errors")
    Assert.assertTrue(result.isSuccess)

    logger.info("The original message should match the result")
    Assert.assertTrue(result.get == originalMessage)
  }

}
