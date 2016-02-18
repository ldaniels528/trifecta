package com.github.ldaniels528.trifecta.modules

import scala.concurrent.ExecutionContext.Implicits.global
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Core Module Specification
 * @author lawrence.daniels@gmail.com
 */
class CoreModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {
  private val config = TxConfig.defaultConfig
  private implicit val zk = mock[ZKProxy]
  private implicit val rt = new TxRuntimeContext(config)(global)
  private val module = new CoreModule(config)

  info("As a Core Module")
  info("I want to be able to execute commands bound to the Core Module")

  feature("Avro Schemas can be loaded into memory") {
    scenario("A file containing an Avro Schemas is loaded into memory") {
      Given("an Avro schema")
      val avroSchema =
        """|{
          |    "type": "record",
          |    "name": "CSVQuoteRecord",
          |    "namespace": "com.shocktrade.avro",
          |    "fields":[
          |        { "name": "symbol", "type":"string", "doc":"stock symbol" },
          |        { "name": "lastTrade", "type":["null", "double"], "doc":"last sale price", "default":null },
          |        { "name": "tradeDate", "type":["null", "long"], "doc":"last sale date", "default":null },
          |        { "name": "tradeTime", "type":["null", "string"], "doc":"last sale time", "default":null },
          |        { "name": "ask", "type":["null", "double"], "doc":"ask price", "default":null },
          |        { "name": "bid", "type":["null", "double"], "doc":"bid price", "default":null },
          |        { "name": "change", "type":["null", "double"], "doc":"price change", "default":null },
          |        { "name": "changePct", "type":["null", "double"], "doc":"price change percent", "default":null },
          |        { "name": "prevClose", "type":["null", "double"], "doc":"previous close price", "default":null },
          |        { "name": "open", "type":["null", "double"], "doc":"open price", "default":null },
          |        { "name": "close", "type":["null", "double"], "doc":"close price", "default":null },
          |        { "name": "high", "type":["null", "double"], "doc":"day's high price", "default":null },
          |        { "name": "low", "type":["null", "double"], "doc":"day's low price", "default":null },
          |        { "name": "volume", "type":["null", "long"], "doc":"day's volume", "default":null },
          |        { "name": "marketCap", "type":["null", "double"], "doc":"market capitalization", "default":null },
          |        { "name": "errorMessage", "type":["null", "string"], "doc":"error message", "default":null }
          |    ],
          |    "doc": "A schema for CSV quotes"
          |}""".stringPrefix
      info(avroSchema)

      When("the avroLoadSchema method is executed")
      //module.avroLoadSchema(UnixLikeArgs(Some("avroload"), List("qschema", "avro/quotes.avsc")))

      Then("the scope should contain the schema variable with its content")

    }
  }

}
