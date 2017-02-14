package com.github.ldaniels528.trifecta.messages.query.parser

import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.query.{IOSource, KQLRestrictions, KQLSelection}
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

    scenario("A simple query (without: specific fields, a where clause or limit)") {
      val queryString = """Select * From "shocktrade.quotes.avro""""
      Given(s"a KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "shocktrade.quotes.avro", decoderURL = None),
        destination = None,
        fields = List("*"),
        criteria = None,
        restrictions = KQLRestrictions(),
        limit = Some(50))
    }

    scenario("A query using 'with default' without a where clause") {
      val queryString =
        """
          |Select symbol, exchange, lastTrade, open, prevClose, high, low, volume
          |From "shocktrade.quotes.avro" With default
          |Limit 50
          |""".stripMargin

      Given(s"KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "shocktrade.quotes.avro", decoderURL = Some("default")),
        destination = None,
        fields = List("symbol", "exchange", "lastTrade", "open", "prevClose", "high", "low", "volume"),
        criteria = None,
        restrictions = KQLRestrictions(),
        limit = Some(50))
    }

    scenario("A query using function - no where clause") {
      val queryString =
        """
          |Select parsejson(request)
          |From "com.shocktrade.accesslogs" With default
          |Limit 50
          |""".stripMargin

      Given(s"KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "shocktrade.quotes.avro", decoderURL = Some("default")),
        destination = None,
        fields = List("parsejson(request)"),
        criteria = None,
        restrictions = KQLRestrictions(),
        limit = Some(50))
    }

    scenario("A query using 'with default' with a where clause") {
      val queryString =
        """
          |Select symbol, exchange, lastTrade, open, prevClose, high, low, volume
          |From "shocktrade.quotes.avro" With default
          |Where volume >= 1,000,000 And lastTrade <= 1
          |Limit 50
          |""".stripMargin
      Given(s"KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "shocktrade.quotes.avro", decoderURL = Some("default")),
        destination = None,
        fields = List("symbol", "exchange", "lastTrade", "open", "prevClose", "high", "low", "volume"),
        criteria = Some(AND(GE("volume", "1000000"), LE("lastTrade", "1"))),
        restrictions = KQLRestrictions(),
        limit = Some(50))
    }
  }

  feature("Ability to parse KQL query") {
    scenario("A query containing fields, a 'where' clause, and a 'using' clause") {
      val queryString =
        """
          |select symbol, exchange, lastTrade, open, prevClose, high, low, volume
          |from "shocktrade.quotes.avro" with "avro:file:avro/quotes.avsc"
          |where volume >= 1,000,000 and lastTrade <= 1
          |using consumer dev_test
          |limit 10
          |""".stripMargin
      Given(s"KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      info(query.toString)
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "shocktrade.quotes.avro", decoderURL = Some("avro:file:avro/quotes.avsc")),
        destination = None,
        fields = List("symbol", "exchange", "lastTrade", "open", "prevClose", "high", "low", "volume"),
        criteria = Some(AND(GE("volume", "1000000"), LE("lastTrade", "1"))),
        restrictions = KQLRestrictions(groupId = Some("dev_test")),
        limit = Some(10))
    }
  }

  feature("Ability to parse KQL queries with an embedded insert") {
    scenario("A string containing a KQL selection query") {
      val queryString =
        """
          |select symbol, exchange, lastTrade, volume
          |from quotes with "avro:file:avro/quotes.avsc"
          |into "es:/quotes/quote/AAPL" with json
          |where exchange == "OTCBB"
          |and lastTrade <= 1.0
          |and volume >= 1,000,000
          |limit 10
          |""".stripMargin
      Given(s"KQL query: $queryString")

      When("The queries is parsed into a KQL object")
      val query = KafkaQueryParser(queryString)

      Then("The arguments should be successfully verified")
      query shouldBe KQLSelection(
        source = IOSource(deviceURL = "quotes", decoderURL = Some("avro:file:avro/quotes.avsc")),
        destination = Some(IOSource(deviceURL = "es:/quotes/quote/AAPL", decoderURL = Some("json"))),
        fields = List("symbol", "exchange", "lastTrade", "volume"),
        criteria = Some(AND(AND(EQ("exchange", "OTCBB"), LE("lastTrade", "1.0")), GE("volume", "1000000"))),
        restrictions = KQLRestrictions(),
        limit = Some(10))
    }
  }

}
