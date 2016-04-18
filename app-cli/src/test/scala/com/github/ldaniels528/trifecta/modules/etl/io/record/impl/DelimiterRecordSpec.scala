package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.record.{DataTypes, Field}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
  * Delimiter Record Specification
  * @author lawrence.daniels@gmail.com
  */
class DelimiterRecordSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {
  val validation = List("symbol" -> Some("AAPL"), "open" -> Some("96.76"), "close" -> Some("96.99"), "low" -> Some("95.89"), "high" -> Some("109.99"))

  info("As a DelimiterRecord instance")
  info("I want to be able to transform text into delimited record (and vice versa)")

  feature("Transform CSV text to CSV record") {
    scenario("Import a CSV stock quote into a CSV record") {
      Given("a text string in CSV format")
      val csvText = """"AAPL", 96.76, 96.99, 95.89, 109.99"""

      And("a CSV record")
      val record = DelimitedRecord(
        id = "cvs_rec",
        delimiter = ',',
        isTextQuoted = true,
        isNumbersQuoted = false,
        fields = Seq(
          Field(name = "symbol", `type` = DataTypes.STRING),
          Field(name = "open", `type` = DataTypes.STRING),
          Field(name = "close", `type` = DataTypes.STRING),
          Field(name = "low", `type` = DataTypes.STRING),
          Field(name = "high", `type` = DataTypes.STRING)
        ))

      And("a scope")
      implicit val scope = new Scope()

      When("the text is consumed")
      val dataSet = record.fromText(csvText)

      Then("the toLine method should return the CSV string")
      val outText = record.toText(dataSet)
      info(outText)
      outText shouldBe """"AAPL","96.76","96.99","95.89","109.99""""

      And(s"the record must contain the values")
      dataSet.data shouldBe validation
    }
  }

  feature("Transform delimited text to delimited record") {
    scenario("Import a delimited stock quote into a delimited record") {
      Given("a text string in delimited format")
      val line = "AAPL\t96.76\t96.99\t95.89\t109.99"

      And("a delimited record")
      val record = DelimitedRecord(
        id = "delim_rec",
        delimiter = '\t',
        fields = Seq(
          Field(name = "symbol", `type` = DataTypes.STRING),
          Field(name = "open", `type` = DataTypes.STRING),
          Field(name = "close", `type` = DataTypes.STRING),
          Field(name = "low", `type` = DataTypes.STRING),
          Field(name = "high", `type` = DataTypes.STRING)
        ))

      And("a scope")
      implicit val scope = new Scope()

      When("the text is consumed")
      val dataSet = record.fromText(line)

      Then("the toLine method should return the delimited string")
      val outText = record.toText(dataSet)
      info(outText)
      outText shouldBe "AAPL\t96.76\t96.99\t95.89\t109.99"

      And(s"the record must contain the values")
      dataSet.data shouldBe validation
    }
  }

}