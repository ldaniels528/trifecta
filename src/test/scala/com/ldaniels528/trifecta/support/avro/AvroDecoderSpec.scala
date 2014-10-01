package com.ldaniels528.trifecta.support.avro

import com.ldaniels528.trifecta.modules.core.FileInputHandler
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData
import com.ldaniels528.trifecta.util.TxUtils._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.{Failure, Success}

/**
 * Avro Decoder Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class AvroDecoderSpec() extends FeatureSpec with GivenWhenThen {

  info("As an Avro Decoder")
  info("I want to be able to interact with Avro schemas")

  feature("Ability to decode Avro messages") {
    scenario("Decode a message containing a stock quote") {
      Given("an Avro Schema")
      val schemaString = Source.fromURL(getClass.getResource("/avro/quotes.avsc")).getLines() mkString "\n"

      And("an Avro Decoder")
      val decoder = AvroDecoder("myDecoder", schemaString)

      When("an Avro-encoded record is loaded")
      val encoded = FileInputHandler("/GDF.bin") use (_.read)

      Then("it should be successfully decoded")
      val record = decoder.decode(encoded.message) match {
        case Success(rec) => rec
        case Failure(e) =>
          throw new IllegalStateException("Quote could not be decoded", e)
      }

      And("fields match the expected value")
      (record.getSchema.getFields map (_.name()) toSeq) shouldBe Seq("symbol", "lastTrade",
        "tradeDate", "tradeTime", "ask", "bid", "change", "changePct", "prevClose", "open",
        "close", "high", "low", "volume", "marketCap", "errorMessage")
    }
  }

}
