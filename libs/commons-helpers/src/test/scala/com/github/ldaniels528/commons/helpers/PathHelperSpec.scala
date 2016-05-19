package com.github.ldaniels528.commons.helpers

import PathHelper._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Path Helper Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class PathHelperSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a PathHelper")
  info("I want to be able to expand UNIX-style paths into JVM-safe values")

  feature("Expand a UNIX-style paths into a JVM accepted value") {

    scenario("Expanding a string containing an UNIX-style path") {
      Given("a string containing an UNIX-style path")
      val path = "~/avro/quotes.avsc"

      When("the path is expanded")
      val jvmPath = expandPath(path)

      Then(s"the result should match the expected value")
      jvmPath shouldBe s"${scala.util.Properties.userHome}/avro/quotes.avsc"
    }

    scenario("Ensure that a string that expansion doesn't affect ineligible paths") {
      Given("a string containing a JVM-safe path")
      val path = "./avro/quotes.avsc"

      When("the path is expanded")
      val jvmPath = expandPath(path)

      Then(s"the result should match the expected value")
      jvmPath shouldBe path
    }

  }


}
