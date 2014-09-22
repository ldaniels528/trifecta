package com.ldaniels528.trifecta.util

import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * End-Point Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EndPointSpec() extends FeatureSpec with GivenWhenThen {

  info("As a EndPoint instance")
  info("I want to be able to encode and/or decode end-points")

  feature("Ability to decode an end-point into a host and port") {
    scenario("Decoding an end-point into a string containing a host and port") {
      Given("A an end-point")
      val host = "someHost"
      val port = 8080
      val endPoint = EndPoint(s"$host:$port")

      When("The end-point object's unapply function is used")
      val (myHost, myPort) = EndPoint.unapply(endPoint)

      Then("The host name should match the original value")
      myHost shouldBe host

      And("The port should match the original value")
      myPort shouldBe port
    }
  }

}
