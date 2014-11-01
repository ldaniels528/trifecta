package com.ldaniels528.trifecta.command.parser.bdql

import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Big Data Query Language (BD-QL) Parser Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class BigDataQueryParserSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Big Data Query Parser")
  info("I want to be able to parse Big Data queries into query objects")

  feature("Ability to parse Big Data queries into BD-QL objects") {
    scenario("A string containing a Big Data selection query") {
      Given("a Big Data selection query")
      val queryString =
        """
          |select symbol, exchange, lastTrade, volume
          |from kafka_queries
          |where exchange = 'OTCBB'
          |and lastTrade <= 1.0
          |and volume >= 1000000
          |limit 10
          | """.stripMargin

      When("The queries is parsed into a BD-QL object")
      val selection = BigDataQueryParser.parse(queryString)

      Then("The arguments should be successfully verified")
      selection shouldBe BigDataSelection(
        source = "kafka_queries",
        fields = List("symbol", "exchange", "lastTrade", "volume"),
        conditions = List(),
        limit = Some(10))
    }
  }

}
