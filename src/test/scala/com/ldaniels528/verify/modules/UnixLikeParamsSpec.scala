package com.ldaniels528.verify.modules

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * Unix-Like Command Parameters Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class UnixLikeParamsSpec() extends FeatureSpec with GivenWhenThen with MockitoSugar {

  info("As a Unix-Like Command Parameter instance")
  info("I want to be able to parse and validate command line input")

  feature("Unix-Like Command Parameter Argument Verification") {
    scenario("The Command Parameter instance should parse and verify command line input into arguments") {
      Given("A Command Parameter Set and command line input")
      val paramSet = UnixLikeParams(defaults = Seq("key" -> true), flags = Seq("-r" -> "recursive"))
      val commandLineInput = "zrm -r /some/path/to/delete"
      val command = mock[Command]

      When("Parsing command line input into arguments")
      val args = CommandParser.parse(commandLineInput)

      Then("The arguments should be successfully verified")
      val params = args.tail
      //intercept[IllegalArgumentException]
      paramSet.checkArgs(command, params)
      assert(paramSet.transform(params) sameElements List("-r" -> List("/some/path/to/delete")))
    }
  }

}
