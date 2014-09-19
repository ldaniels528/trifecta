package com.ldaniels528.verify.vscript

import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * VScript Parser Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VScriptParserSpec extends FeatureSpec with GivenWhenThen {
  private val QUOTES = "\"\"\""

  info("As a VScript Parser")
  info("I want to be able to parse VScript expressions into tokens")

  feature("Parsing an array variable assignment into tokens") {
    scenario("Parse an expression containing an array variable assignment") {
      Given("an array variable assignment expression")
      val expression = "val a0 = [2, 3, 4]"

      When("the expression is parsed into tokens")
      val tokens = VScriptParser.parse(expression, debug = false)

      And("the results are extracted from the tokens")
      val results = tokens.toSeq
      showResults(results)

      Then("the tokens should match the expected results")
      results shouldBe Seq("val", "a0", "=", "[", "2", ",", "3", ",", "4", "]")
    }
  }

  feature("Parsing a function declaration into tokens") {
    scenario("Parse an expression containing function declaration") {
      Given("a function declaration expression")
      val expression = "def doAdd(a, b) { a + b }"

      When("the expression is parsed into tokens")
      val tokens = VScriptParser.parse(expression, debug = false)

      And("the results are extracted from the tokens")
      val results = tokens.toSeq
      showResults(results)

      Then("the tokens should match the expected results")
      results shouldBe Seq("def", "doAdd", "(", "a", ",", "b", ")", "{", "a", "+", "b", "}")
    }
  }

  feature("Parsing a function call into tokens") {
    scenario("Parse an expression containing function call") {
      Given("a function call expression")
      val expression = """
      	val y = 3
        val x = 2 * y + 5
        println doAdd(x,y) """

      When("the expression is parsed into tokens")
      val tokens = VScriptParser.parse(expression, debug = false)

      And("the results are extracted from the tokens")
      val results = tokens.toSeq
      showResults(results)

      Then("the tokens should match the expected results")
      results shouldBe Seq(
        "val", "y", "=", "3", "\n",
        "val", "x", "=", "2", "*", "y", "+", "5", "\n",
        "println", "doAdd", "(", "x", ",", "y", ")"
      )
    }
  }

  def showResults(results: Seq[String]): Unit = {
    info(s"results are $QUOTES${results.map(mapper).mkString(" ")}$QUOTES")
  }

  def mapper(s: String): String = if (s == "\n") "\\n" else s

}
