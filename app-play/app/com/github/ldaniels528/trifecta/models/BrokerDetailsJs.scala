package com.github.ldaniels528.trifecta.models

import com.github.ldaniels528.trifecta.io.kafka.KafkaMicroConsumer.BrokerDetails
import play.api.libs.json.Json

/**
  * Broker Details JSON model
  * @author lawrence.daniels@gmail.com
  */
case class BrokerDetailsJs(jmx_port: Int, timestamp: String, host: String, version: Int, port: Int)

/**
  * Broker Details JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object BrokerDetailsJs {

  implicit val BrokerReads = Json.reads[BrokerDetailsJs]

  implicit val BrokerWrites = Json.writes[BrokerDetailsJs]

  /**
    * Broker Details Conversion
    * @param details the given [[BrokerDetails broker details]]
    */
  implicit class BrokerDetailsConversion(val details: BrokerDetails) extends AnyVal {

    def asJson = BrokerDetailsJs(
      jmx_port = details.jmx_port,
      timestamp = details.timestamp,
      host = details.host,
      version = details.version,
      port = details.port
    )
  }

}
