package com.ldaniels528.verify.subsystems.kafka

import com.ldaniels528.verify.io.EndPoint

/**
 * Type-safe Broker definition
 * @author lawrence.daniels@gmail.com
 */
class Broker(host: String, port: Int) extends EndPoint(host, port)

object Broker {

  def apply(host: String, port: Int = -1) = new Broker(host, port)

  def apply(pair: String): Broker = {
    pair.split(":").toList match {
      case host :: port :: Nil => new Broker(host, port.toInt)
      case _ =>
        throw new IllegalStateException(s"Invalid end-point '$pair'; Format 'host:port' expected")
    }
  }

  def unapply(e: Broker) = (e.host, e.port)

}