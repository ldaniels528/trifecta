package com.ldaniels528.trifecta.io.kafka

/**
 * Type-safe Broker representation
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class Broker(host: String, port: Int, brokerId: Int = 0)   {

  override def toString = s"$host:$port"

}