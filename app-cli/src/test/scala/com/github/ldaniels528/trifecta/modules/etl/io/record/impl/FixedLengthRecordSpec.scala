package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.record.{DataTypes, Field}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
  * Fixed-Length Record Specification
  * @author lawrence.daniels@gmail.com
  */
class FixedLengthRecordSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a FixedLengthRecord instance")
  info("I want to be able to transform text into a fixed-length string (and vice versa)")

  feature("Transforms text to fixed-length records") {
    scenario("Import a fixed-length stock quote into a fixed-length record") {
      Given("a text string in fixed-length format")
      val inText = "AAPL      96.76     96.99     95.89     109.99"

      And("a fixed-length record")
      val record = FixedRecord(
        id = "fixed_rec",
        fields = Seq(
          Field(name = "symbol", `type` = DataTypes.STRING, length = 10),
          Field(name = "open", `type` = DataTypes.STRING, length = 10),
          Field(name = "close", `type` = DataTypes.STRING, length = 10),
          Field(name = "low", `type` = DataTypes.STRING, length = 10),
          Field(name = "high", `type` = DataTypes.STRING, length = 10)
        ))

      And("a scope")
      implicit val scope = new Scope()

      When("the text is consumed")
      val dataSet = record.fromText(inText)
      dataSet.data foreach {
        case (name, Some(value)) =>
          info(s"name: $name, value: '$value'")
        case (name, None) =>
          info(s"name: $name, value is null")
      }

      Then("the toText method should return the fixed-length string")
      val outText = record.toText(dataSet)
      info(s"[$outText]")
      outText.trim shouldBe inText

      And(s"the record must contain the values")
      val validation = List("symbol" -> "AAPL", "open" -> "96.76", "close" -> "96.99", "low" -> "95.89", "high" -> "109.99")
      dataSet.data shouldBe validation.map { case (k, v) => (k, Option(v)) }
    }
  }

}