package com.github.ldaniels528.trifecta

import com.github.ldaniels528.trifecta.TxConsole._
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Color._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Trifecta Console Specification
 * @author lawrence.daniels@gmail.com
 */
class TxConsoleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a TxConsole instance")
  info("I want to be able to print escape characters")

  feature("Printing an ANSI string interpolation creates ANSI colored sequences") {
    scenario("An ANSI interpolation contains multiple escape sequences") {
      Given("An ANSI interpolation containing escape sequences for colors red, green, blue and white")
      val interpolation = a"${RED}Ve${GREEN}ri${BLUE}fy ${WHITE}v0.10"

      When("the interpolation is converted to a string")
      val ansiString = interpolation.toString

      Then(s"the string should an instance of ${classOf[Ansi].getName}")
      assert(interpolation.isInstanceOf[Ansi])

      And(s"the string should contain the escape sequences")
      ansiString shouldBe "\u001B[31mVe\u001B[32mri\u001B[34mfy \u001B[37mv0.10\u001B[m"
    }
  }

}
