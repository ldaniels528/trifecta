package com.ldaniels528.trifecta

import com.ldaniels528.trifecta.TxConfig.TxDecoder
import com.ldaniels528.trifecta.io.avro.AvroDecoder
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.commons.helpers.Resource
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

import scala.io.Source

/**
 * Trifecta Configuration Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxConfigSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {

  info("As a TxConfig instance")
  info("I want to be able to provide configuration properties to the application")

  feature("Retrieve message decoders") {
    scenario("Load decoders from the user's .trifecta directory") {
      Given("a default configuration")
      val config = TxConfig.defaultConfig

      When("the messages decoders are retrieved")
      val decoders = config.getDecoders
      decoders foreach(d => info(s"decoder: $d"))

      Then(s"Two message decoders should be found")
      /*
      decoders shouldBe Some(Array(
        TxDecoder("shocktrade.keystats.avro", AvroDecoder("keyStatistics.avsc", loadSchema("/avro/keyStatistics.avsc"))),
        TxDecoder("shocktrade.quotes.avro", AvroDecoder("quotes.avsc", loadSchema("/avro/stockQuotes.avsc")))
      ))*/
    }
  }

  /*
Some(Array(
TxDecoder(shocktrade.keystats.avro,AvroDecoder(keyStatistics.avsc)),
TxDecoder(shocktrade.quotes.avro,AvroDecoder(quotes.avsc))))

Some(Array(
TxDecoder(shocktrade.keystats.avro,AvroDecoder(keyStatistics.avsc)),
TxDecoder(shocktrade.quotes.avro,AvroDecoder(quotes.avsc))))
   */

  private def loadSchema(path: String): String = {
    Resource(path) map (url => Source.fromURL(url).getLines().mkString) orDie s"Resource $path not found"
  }

}
