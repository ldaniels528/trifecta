package com.github.ldaniels528.trifecta.io.json

import com.github.ldaniels528.trifecta.io.json.JsonHelper._
import com.mongodb.casbah.Imports.{DBObject => Q}
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
 * JSON Helper Utility Specification
 * @author lawrence.daniels@gmail.com
 */
class JsonHelperSpec() extends FeatureSpec with GivenWhenThen {

  feature("The ability to parse a JSON string into an object") {
    scenario("Parse a JSON string into a JValue") {
      Given("a JSON string")
      val jsonString =
        """
          |    {
          |        "topology":{
          |          "id":"a46e3b1e-8a67-4aa0-9d8e-44b4e9bad9b9",
          |          "name":"NetworkMonitoringTrafficRateAggregation"
          |        },
          |        "offset":43710000,
          |        "partition":1,
          |        "broker":{
          |          "host":"vsccrtc204-brn1.rtc.vrsn.com",
          |          "port":9092
          |        },
          |        "topic":"Verisign.test.iota.rtc.listener.all.hydra"
          |    }""".stripMargin

      When("the JSON string is parsed")
      val doc = toDocument(toJson(jsonString))

      Then("the result matches the expected result")
      doc shouldBe Q("partition" -> 1, "broker" -> Q("host" -> "vsccrtc204-brn1.rtc.vrsn.com", "port" -> 9092),
        "offset" -> 43710000, "topology" -> Q("id" -> "a46e3b1e-8a67-4aa0-9d8e-44b4e9bad9b9",
          "name" -> "NetworkMonitoringTrafficRateAggregation"), "topic" -> "Verisign.test.iota.rtc.listener.all.hydra")
    }
  }

}
