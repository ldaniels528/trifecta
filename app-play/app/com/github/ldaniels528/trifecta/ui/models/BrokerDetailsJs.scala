package com.github.ldaniels528.trifecta.ui.models

import com.github.ldaniels528.trifecta.io.kafka.KafkaZkUtils.BrokerDetails
import play.api.libs.json.{Json, Reads, Writes}

import scala.util.Try

/**
  * Broker Details JSON model
  * @author lawrence.daniels@gmail.com
  */
case class BrokerDetailsJs(jmx_port: Int, timestamp: Option[Long], timestampISO: Option[String], host: String, version: Int, port: Int)

/**
  * Broker Details JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object BrokerDetailsJs {

  implicit val BrokerReads: Reads[BrokerDetailsJs] = Json.reads[BrokerDetailsJs]

  implicit val BrokerWrites: Writes[BrokerDetailsJs] = Json.writes[BrokerDetailsJs]

  /**
    * Broker Details Conversion
    * @param details the given [[BrokerDetails broker details]]
    */
  implicit class BrokerDetailsConversion(val details: BrokerDetails) extends AnyVal {

    def asJson = BrokerDetailsJs(
      jmx_port = details.jmx_port,
      timestamp = Try(details.timestamp.toLong).toOption,
      timestampISO = details.timestampISO,
      host = details.host,
      version = details.version,
      port = details.port
    )
  }

}
