package com.github.ldaniels528.trifecta.messages.query.parser

import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Kafka Query Language (KQL) Tokenizer Specification
 * @author lawrence.daniels@gmail.com
 */
class KafkaQueryTokenizerSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Kafka Query Tokenizer")
  info("I want to be able to parse Kafka queries into query objects")

  feature("Ability to parse Kafka queries into KQL objects") {
    scenario("A string containing a Kafka selection query") {
      Given("a Kafka query string")
      val queryString =
        """
          |select symbol, exchange, lastTrade, volume
          |from "topic:shocktrade.quotes.avro" with "avro:file:~/avro/quotes.avsc"
          |into elastic_search_quotes
          |where exchange = 'OTCBB'
          |and lastTrade <= 1.0
          |and volume >= 1,000,000
          |limit 10
          | """.stripMargin

      When("The query is parsed into tokens")
      val tokens = KafkaQueryTokenizer.parse(queryString)

      Then("The arguments should be successfully verified")
      info(s"results: ${tokens map (s => s""""$s"""") mkString " "}")
      tokens shouldBe Seq(
        "select", "symbol", ",", "exchange", ",", "lastTrade", ",", "volume", "from",
        "\"topic:shocktrade.quotes.avro\"", "with", "\"avro:file:~/avro/quotes.avsc\"",
        "into", "elastic_search_quotes", "where", "exchange", "=", "'OTCBB'", "and", "lastTrade", "<=",
        "1.0", "and", "volume", ">=", "1,000,000", "limit", "10")
    }
  }

}
