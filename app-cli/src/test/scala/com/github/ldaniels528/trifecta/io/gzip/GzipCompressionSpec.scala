package com.github.ldaniels528.trifecta.io.gzip

import com.github.ldaniels528.trifecta.io.gzip.GzipCompression._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * GZIP Compression Specification
 * @author lawrence.daniels@gmail.com
 */
class GzipCompressionSpec() extends FeatureSpec with GivenWhenThen with GzipCompression {

  info("As a GzipCompression instance")
  info("I want to be able to compress and/or decompress data")

  feature("Ability to compress and decompress data") {
    scenario("Compress then decompress a message") {
      Given("A message and a character encoding")
      val originalMessage = "This is a sample sentence to compress"
      val encoding = "UTF8"

      When("The message is compressed then decompressed")
      val result = for {
        compressedData <- originalMessage.compress(encoding)
        uncompressedData <- compressedData.decompress
      } yield new String(uncompressedData, encoding)

      Then("The operation should have completed without errors")
      assert(result.isSuccess)

      And("The original message should be the same as the decompressed message")
      result.get shouldBe originalMessage
    }
  }

}
