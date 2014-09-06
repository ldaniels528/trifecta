package com.ldaniels528.verify.util

import com.ldaniels528.verify.util.GzipCompression._
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory

/**
 * GzipCompression Test Suite
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class GzipCompressionTest {
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
