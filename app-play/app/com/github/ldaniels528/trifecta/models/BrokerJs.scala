package com.github.ldaniels528.trifecta.models

import com.github.ldaniels528.trifecta.io.kafka.Broker
import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import play.api.libs.json.Json

/**
  * Broker JSON model
  * @author lawrence.daniels@gmail.com
  */
case class BrokerJs(host: Option[String], port: Option[Int], brokerId: Option[Int])

/**
  * Broker JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object BrokerJs {

  implicit val BrokerReads = Json.reads[BrokerJs]

  implicit val BrokerWrites = Json.writes[BrokerJs]

  /**
    * Broker Conversion
    * @param broker the given [[BrokerJs broker]]
    */
  implicit class BrokerConversion(val broker: Broker) extends AnyVal {

    def asJson = BrokerJs(host = broker.host, port = broker.port, brokerId = broker.brokerId)

  }

}