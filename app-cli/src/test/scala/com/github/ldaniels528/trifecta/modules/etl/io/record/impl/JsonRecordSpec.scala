package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.record.{DataTypes, Field}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
  * Json Record Specification
  * @author lawrence.daniels@gmail.com
  */
class JsonRecordSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a JsonRecord instance")
  info("I want to be able to transform text into JSON record (and vice versa)")

  feature("Transform JSON text into a JSON record") {
    scenario("Import a JSON stock quote into a JSON record") {
      Given("a text string in JSON format")
      val jsonString = """{ "symbol":"AAPL", "open":96.76, "close":96.99, "low":95.89, "high":109.99 }"""

      And("a JSON record")
      val record = JsonRecord(
        id = "json_rec",
        fields = Seq(
          Field(name = "symbol", `type` = DataTypes.STRING),
          Field(name = "open", `type` = DataTypes.DOUBLE),
          Field(name = "close", `type` = DataTypes.DOUBLE),
          Field(name = "low", `type` = DataTypes.DOUBLE),
          Field(name = "high", `type` = DataTypes.DOUBLE)
        ))

      And("a scope")
      implicit val scope = new Scope()

      When("the text is consumed")
      val dataSet = record.fromText(jsonString)
      dataSet.data foreach {
        case (name, Some(value)) =>
          info(s"name: $name, value: '$value'")
        case (name, None) =>
          info(s"name: $name, value is null")
      }

      Then("the toJson method should return the JSON string")
      val jsonOutput = record.toJson(dataSet).toString()
      info(jsonOutput)
      jsonOutput shouldBe """{"symbol":"AAPL","open":96.76,"close":96.99,"low":95.89,"high":109.99}"""

      And(s"the record must contain the values")
      val validation = List("symbol" -> Some("AAPL"), "open" -> Some(96.76d), "close" -> Some(96.99d), "low" -> Some(95.89d), "high" -> Some(109.99d))
      info(s"dataSet => ${dataSet.data}")
      dataSet.data.toSet shouldBe validation.toSet
    }
  }

}
