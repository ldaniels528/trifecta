package com.ldaniels528.verify.support.kafka

import com.ldaniels528.verify.io.EndPoint

/**
 * Type-safe Broker implementation
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class Broker(host: String, port: Int, brokerId: Int = 0) extends EndPoint