package com.ldaniels528.verify.command

import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Simple Command Parameters Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class SimpleParamsSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {

  info("As a Simple Command Parameter instance")
  info("I want to be able to parse and validate command line input")

  feature("Simple Command Parameter Argument Verification") {
    scenario("The Command Parameter instance should parse and verify command line input into arguments") {
      Given("A Command Parameter Set and command line input")
      val paramSet = SimpleParams(required = Seq("key"), optional = Seq("recursive"))
      val commandLineInput = "zrm -r /some/path/to/delete"
      val command = mock[Command]

      When("Parsing command line input into arguments")
      val args = CommandParser.parseTokens(commandLineInput)

      Then("The arguments should be successfully verified")
      paramSet.checkArgs(command, args)
      paramSet.transform(args) shouldBe Seq("zrm", "-r", "/some/path/to/delete")
    }
  }

}
