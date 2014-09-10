package com.ldaniels528.verify.modules

import com.ldaniels528.verify.modules.CommandParser.UnixLikeArgs
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Command Parser Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CommandParserSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {

  info("As a Command Parser")
  info("I want to be able to parse command line input into tokens")

  feature("Ability to distinguish symbols from atoms") {
    scenario("A string contains both symbols and atoms") {
      Given("A string containing symbols and atoms")
      val line = "!?100"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parse(line)

      Then("The arguments should be successfully verified")
      assert(tokens sameElements Seq("!", "?", "100"))
    }
  }

  feature("Ability to parse a string of mixed tokens (atoms, operators and symbols)") {
    scenario("A string contains both atoms, operators and symbols") {
      Given("A string containing atoms, operators and symbols")
      val line = "kdumpa avro/schema.avsc 9 1799020 a+b+c+d+e+f"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parse(line)

      Then("The arguments should be successfully verified")
      assert(tokens sameElements Seq("kdumpa", "avro/schema.avsc", "9", "1799020", "a+b+c+d+e+f"))
    }
  }

  feature("Ability to parse a string into Unix-style parameters with flags)") {
    scenario("A string contains Unix-style parameters") {
      Given("A string containing Unix-style parameters with flags")
      val line = "kget -a schema -f outfile.txt shocktrades.quotes.csv 0 165 -b"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parse(line)

      Then("The tokens are transformed into Unix-style parameters")
      val result = CommandParser.parseUnixLikeArgs(tokens)

      And("Finally validate the Unix-style parameters")
      result shouldBe UnixLikeArgs(List("kget", "shocktrades.quotes.csv", "0", "165"), Map("-f" -> Some("outfile.txt"), "-a" -> Some("schema"), "-b" -> None))
    }
  }

  feature("Ability to parse a string into Unix-style parameters without flags)") {
    scenario("A string contains Unix-style parameters") {
      Given("A string containing Unix-style parameters without flags")
      val line = "kget shocktrades.quotes.csv 0 165"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parse(line)

      Then("The tokens are transformed into Unix-style parameters")
      val result = CommandParser.parseUnixLikeArgs(tokens)

      And("Finally validate the Unix-style parameters")
      result shouldBe UnixLikeArgs(List("kget", "shocktrades.quotes.csv", "0", "165"))
    }
  }

}