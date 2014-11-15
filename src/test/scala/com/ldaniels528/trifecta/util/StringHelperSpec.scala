package com.ldaniels528.trifecta.util

import com.ldaniels528.trifecta.util.StringHelper._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * String Helper Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class StringHelperSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a StringHelper")
  info("I want to be able to perform various String operations in an idiomatic type-safe way")

  feature("Identify the position of a substring for a given string") {

    scenario("A string containing a partially matching substring") {
      Given("A source string and a partially matching substring")
      val stringA = "A little brown fox climb the hill"
      val stringB = "fox"

      When("the indexOptionOf method is called")
      val index = stringA.indexOptionOf(stringB)

      Then(s"the result should match the expected value")
      index shouldBe Some(15)
    }

    scenario("Two strings that do not contain a match") {
      Given("Two distinctly different strings")
      val stringA = "A little brown fox climb the hill"
      val stringB = "cat"

      When("the indexOptionOf method is called")
      val index = stringA.indexOptionOf(stringB)

      Then(s"the result should be None")
      index shouldBe None
    }
  }

}
