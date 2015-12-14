package com.github.ldaniels528.trifecta.messages.query.parser

import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.query.{IOSource, KQLSelection}
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Kafka Query Language (KQL) Parser Specification
 * @author lawrence.daniels@gmail.com
 */
class KafkaQueryParserSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Kafka Query Parser")
  info("I want to be able to parse Kafka queries")

  feature("Ability to parse KQL queries with implicit decoders") {
    scenario("A string containing a KQL selection query using 'with default'") {
      Given("a KQL selection query")
      val queryString =
        """
          |Select symbol, exchange, lastTrade, open, prevClose, high, low, volume
          |From "topic:shocktrade.quotes.avro" With default
          |Where volume >= 1,000,000 And lastTrade <= 1
          |Limit 50
          |""".stripMargin

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "topic:shocktrade.quotes.avro", decoderURL = Some("default")),
        destination = None,
        fields = List("symbol", "exchange", "lastTrade", "open", "prevClose", "high", "low", "volume"),
        criteria = Some(AND(GE("volume", "1000000"), LE("lastTrade", "1"))),
        limit = Some(50))
    }
  }

  feature("Ability to parse KQL query") {
    scenario("A string containing a KQL selection query") {
      Given("a KQL selection query")
      val queryString =
        """
          |select symbol, exchange, lastTrade, open, prevClose, high, low, volume
          |from "topic:shocktrade.quotes.avro" with "avro:file:avro/quotes.avsc"
          |where volume >= 1,000,000 and lastTrade <= 1
          |""".stripMargin

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "topic:shocktrade.quotes.avro", decoderURL = Some("avro:file:avro/quotes.avsc")),
        destination = None,
        fields = List("symbol", "exchange", "lastTrade", "open", "prevClose", "high", "low", "volume"),
        criteria = Some(AND(GE("volume", "1000000"), LE("lastTrade", "1"))),
        limit = None)
    }
  }

  feature("Ability to parse KQL queries with an embedded insert") {
    scenario("A string containing a KQL selection query") {
      Given("a KQL selection query")
      val queryString =
        """
          |select symbol, exchange, lastTrade, volume
          |from "topic:quotes" with "avro:file:avro/quotes.avsc"
          |into "es:/quotes/quote/AAPL" with json
          |where exchange == "OTCBB"
          |and lastTrade <= 1.0
          |and volume >= 1,000,000
          |limit 10
          |""".stripMargin

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "topic:quotes", decoderURL = Some("avro:file:avro/quotes.avsc")),
        destination = Some(IOSource(deviceURL = "es:/quotes/quote/AAPL", decoderURL = Some("json"))),
        fields = List("symbol", "exchange", "lastTrade", "volume"),
        criteria = Some(AND(AND(EQ("exchange", "OTCBB"), LE("lastTrade", "1.0")), GE("volume", "1000000"))),
        limit = Some(10))
    }
  }

}
