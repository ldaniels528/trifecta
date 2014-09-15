package com.ldaniels528.verify

import com.ldaniels528.verify.VxConsole._
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Color._
import org.mockito.Mockito.{when => when$}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Verify Console Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VxConsoleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a VxConsole instance")
  info("I want to be able to print escape characters")

  feature("Printing string interpolation creates ANSI colored sequences") {
    scenario("A string interpolation contains multiple escape sequences") {
      Given("A string interpolation containing escape sequences for colors red, green, blue and white")
      val VERSION = "0.10"
      val interpolation = a"${RED}Ve${GREEN}ri${BLUE}fy ${WHITE}v$VERSION"

      When("the interpolation is converted to a string")
      val ansiString = interpolation.toString

      Then(s"the string should an instance of ${classOf[Ansi].getName}")
      assert(interpolation.isInstanceOf[Ansi])

      And(s"the string should contain the escape sequences")
      ansiString shouldBe "\u001B[31mVe\u001B[32mri\u001B[34mfy \u001B[37mv0.10\u001B[m"
    }
  }

}
