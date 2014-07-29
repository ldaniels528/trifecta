package com.ldaniels528.verify.subsystems.storm

import com.ldaniels528.verify.subsystems.kafka.Broker
import com.ldaniels528.verify.io.EndPoint

/**
 * Verify Storm Job Configuration
 * @author lawrence.daniels@gmail.com
 */
case class VerifyStormJobConfig(
  cassandra: EndPoint,
  elasticSearch: EndPoint,
  kafka: Seq[Broker],
  playReact: Seq[EndPoint],
  playPublish: Seq[EndPoint],
  zooKeeper: EndPoint) 