package com.ldaniels528.trifecta.command.parser

import com.ldaniels528.trifecta.command.parser.bdql.BigDataQueryTokenizer
import com.ldaniels528.trifecta.support.messaging.logic.Condition
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.{Success, Try}

/**
 * Token Stream Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TokenStreamSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Token Stream")
  info("I want to be able to extract/process tokens as a stream")

  feature("Ability to parse JSON streams") {
    scenario("A sequence of tokens from a BDQL query") {
      Given("A sequence of tokens from a parsed selection query")
      val tokens = BigDataQueryTokenizer.parse(
        """
          |select symbol, exchange, lastTrade, volume
          |from kafka_queries
          |where exchange = 'OTCBB'
          |and lastTrade <= 1.0
          |and volume >= 1000000
          |limit 10
          | """.stripMargin
      )

      And("a token stream")
      val ts = TokenStream(tokens)

      When("'select' is expected")
      val expect1 = Try(ts.expect("select"))

      Then("the 'expect(\"select\")' function should complete with errors")
      expect1 shouldBe Success(())

      When("the fields are extracted")
      val fields = ts.getUntil(token = "from", delimiter = Option(",")).toList

      Then("the fields should match the expected values")
      fields shouldBe Seq("symbol", "exchange", "lastTrade", "volume")

      // expect: from <source>
      When("'from' is expected")
      val expect2 = Try(ts.expect("from"))

      Then("the 'expect(\"from\")' function should complete with errors")
      expect2 shouldBe Success(())

      When("the source is extracted")
      val source = ts.getOrElse(throw new IllegalArgumentException("Query source expected"))

      Then("the source should match the expected value")
      source shouldBe "kafka_queries"

      When("the conditions are extracted")
      val conditions = ts.ifNext("where") {
        ts.getUntil("limit").foldLeft[Seq[Condition]](Nil) { (criteria, token) =>
          criteria
        }
      }

      Then("the conditions should match the expected values")
      conditions shouldBe Some(List())

      When("the limit is extracted")
      val limit = ts.ifNext("limit") {
        ts.next().toInt
      }

      Then("the limit should match the expected value")
      limit shouldBe Some(10)
    }
  }

}
