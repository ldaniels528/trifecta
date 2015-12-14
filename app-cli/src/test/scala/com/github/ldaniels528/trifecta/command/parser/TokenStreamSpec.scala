package com.github.ldaniels528.trifecta.command.parser

import com.github.ldaniels528.trifecta.messages.logic.Condition
import com.github.ldaniels528.trifecta.messages.query.parser.KafkaQueryTokenizer
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.{Success, Try}

/**
 * Token Stream Specification
 * @author lawrence.daniels@gmail.com
 */
class TokenStreamSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Token Stream")
  info("I want to be able to extract/process tokens as a stream")

  feature("Ability to parse JSON streams") {
    scenario("A sequence of tokens from a KQL query") {
      Given("a sequence of tokens from a parsed selection query")
      val tokens = KafkaQueryTokenizer.parse(
        """
          |select symbol, exchange, lastTrade, volume
          |from kafka_queries
          |where exchange = 'OTCBB'
          |and lastTrade <= 1.0
          |and volume >= 1,000,000
          |limit 10
          | """.stripMargin
      )

      And("a token stream")
      val ts = TokenStream(tokens)

      When("keyword 'select' is expected")
      val expect1 = Try(ts.expect("select"))

      Then("the 'expect(\"select\")' function should complete without errors")
      expect1 shouldBe Success(())

      When("the fields are extracted")
      val fields = ts.getUntil(token = "from", delimiter = Option(",")).toList

      Then("the fields should match the expected values")
      fields shouldBe Seq("symbol", "exchange", "lastTrade", "volume")

      // expect: from <source>
      When("keyword 'from' is expected")
      val expect2 = Try(ts.expect("from"))

      Then("the 'expect(\"from\")' function should complete without errors")
      expect2 shouldBe Success(())

      When("the source is extracted from the 'from' clause")
      val source = ts.getOrElse(throw new IllegalArgumentException("Query source expected"))

      Then("the source should match the expected value")
      source shouldBe "kafka_queries"

      When("the conditions are extracted from the 'where' clause")
      val conditions = ts.ifNext("where") {
        ts.getUntil("limit").foldLeft[Seq[Condition]](Nil) { (criteria, token) =>
          criteria
        }
      }

      Then("the conditions should match the expected values")
      conditions shouldBe Some(List())

      When("the limit is extracted from the 'limit' clause")
      val limit = ts.ifNext("limit") {
        ts.getOrElse(throw new IllegalArgumentException("Limit value expected near 'limit'")).toInt
      }

      Then("the limit should match the expected value")
      limit shouldBe Some(10)
    }
  }

}
