package com.ldaniels528.verify.support.gzip

import com.ldaniels528.verify.support.gzip.GzipCompression._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * GZIP Compression Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class GzipCompressionSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar with GzipCompression {

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
      assert(result.get == originalMessage)
    }
  }

}
