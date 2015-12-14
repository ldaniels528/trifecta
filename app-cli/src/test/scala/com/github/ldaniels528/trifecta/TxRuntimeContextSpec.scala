package com.github.ldaniels528.trifecta

import com.github.ldaniels528.trifecta.command.parser.CommandParser
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Trifecta Runtime Context Specification
 * @author lawrence.daniels@gmail.com
 */
class TxRuntimeContextSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {

  info("As a Runtime Context")
  info("I want to be able to parse, validate and execute module commands")

  feature("Unix-Like Command Parameter Argument Verification") {
    scenario("The Command Parameter instance should parse and trifecta command line input into arguments") {
      Given("A Command Parameter Set and command line input")
      val paramSet = UnixLikeParams(defaults = Seq("key" -> false), flags = Seq("-r" -> "recursive"))
      val commandLineInput = "zrm -r /some/path/to/delete"
      val command = mock[Command]

      When("Parsing command line input into arguments")
      val params = CommandParser.parseTokens(commandLineInput)

      Then("The arguments should be successfully verified")
      //paramSet.checkArgs(command, params)
      paramSet.transform(params) shouldBe UnixLikeArgs(Some("zrm"), Nil, Map("-r" -> Some("/some/path/to/delete")))
    }
  }

}
