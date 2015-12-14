package com.github.ldaniels528.trifecta.command.parser

import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Command Parser Specification
 * @author lawrence.daniels@gmail.com
 */
class CommandParserSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Command Parser")
  info("I want to be able to parse command line input into tokens")

  feature("Ability to parse JSON streams") {
    scenario("A string containing JSON data including quoted string") {
      Given("a string containing JSON data and atoms")
      val line = """eput bucket randomData 1234 { 1 : [ "A", "B", "C" ], 2 : [ "D", "E", "F" ] }  """

      When("The string is parsed into tokens")
      val tokens = CommandParser.parseTokens(line)

      Then("The arguments should be successfully verified")
      info(s"results: ${tokens mkString " "}")
      tokens shouldBe Seq("eput", "bucket", "randomData", "1234", """{ 1 : [ "A", "B", "C" ], 2 : [ "D", "E", "F" ] }""")
    }
  }

  feature("Ability to distinguish symbols from atoms") {
    scenario("A string contains both symbols and atoms") {
      Given("a string containing symbols and atoms")
      val line = "!?100+1"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parseTokens(line)

      Then("The arguments should be successfully verified")
      info(s"results: ${tokens mkString " "}")
      tokens shouldBe Seq("!", "?", "100+1")
    }
  }

  feature("Ability to parse a string of mixed tokens (atoms, operators, symbols and labels)") {
    scenario("A string contains both atoms, operators and symbols") {
      Given("a string containing atoms, operators and symbols")
      val line = """kdumpa avro/schema.avsc `backticks` 9 1799020 a+b+c+d+e+f "Hello World""""

      When("The string is parsed into tokens")
      val tokens = CommandParser.parseTokens(line)

      Then("The arguments should be successfully verified")
      info(s"results: ${tokens mkString " "}")
      tokens shouldBe Seq("kdumpa", "avro/schema.avsc", "`backticks`", "9", "1799020", "a+b+c+d+e+f", "Hello World")
    }
  }

}