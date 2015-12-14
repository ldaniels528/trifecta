package com.github.ldaniels528.trifecta.command

import com.github.ldaniels528.trifecta.command.parser.CommandParser
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Unix-Like Command Parameters Specification
 * @author lawrence.daniels@gmail.com
 */
class UnixLikeParamsSpec() extends FeatureSpec with GivenWhenThen {

  info("As a Unix-Like Command Parameter instance")
  info("I want to be able to parse and validate command line input")

  feature("Ability to parse a string into Unix-style parameters with flags)") {
    scenario("A string contains Unix-style parameters") {
      Given("A string containing Unix-style parameters with flags")
      val line = "kget -a schema -f outfile.txt shocktrades.quotes.csv 0 165 -b"

      When("The string is parsed into tokens")
      val result = CommandParser.parseUnixLikeArgs(line)

      And("Finally validate the Unix-style parameters")
      result shouldBe UnixLikeArgs(Some("kget"), List("shocktrades.quotes.csv", "0", "165"), Map("-f" -> Some("outfile.txt"), "-a" -> Some("schema"), "-b" -> None))
    }
  }

  feature("Ability to parse a string into Unix-style parameters without flags)") {
    scenario("A string contains Unix-style parameters") {
      Given("A string containing Unix-style parameters without flags")
      val line = "kget shocktrades.quotes.csv 0 165"

      When("The string is parsed into tokens")
      val result = CommandParser.parseUnixLikeArgs(line)

      And("Finally validate the Unix-style parameters")
      result shouldBe UnixLikeArgs(Some("kget"), List("shocktrades.quotes.csv", "0", "165"))
    }
  }

}
