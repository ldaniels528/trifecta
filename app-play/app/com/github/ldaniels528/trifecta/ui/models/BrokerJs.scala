package com.github.ldaniels528.trifecta.ui.models

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.io.kafka.Broker
import play.api.libs.json.{Json, Reads, Writes}

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

  implicit val BrokerReads: Reads[BrokerJs] = Json.reads[BrokerJs]

  implicit val BrokerWrites: Writes[BrokerJs] = Json.writes[BrokerJs]

  /**
    * Broker Conversion
    * @param broker the given [[BrokerJs broker]]
    */
  implicit class BrokerConversion(val broker: Broker) extends AnyVal {

    def asJson = BrokerJs(host = broker.host, port = broker.port, brokerId = broker.brokerId)

  }

}