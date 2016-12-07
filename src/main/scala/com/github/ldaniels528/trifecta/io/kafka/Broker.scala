package com.github.ldaniels528.trifecta.io.kafka

/**
  * Type-safe Broker representation
  * @author lawrence.daniels@gmail.com
  */
case class Broker(host: String, port: Int, brokerId: Int = 0) {

  override def toString = s"$host:$port"

}

/**
  * Broker Singleton
  * @author lawrence.daniels@gmail.com
  */
object Broker {

  /**
    * Parses the given broker list into a collection of broker instances
    * @param brokerList the given broker list (e.g. "localhost:9091,localhost:9092,localhost:9093")
    * @return a collection of [[Broker]] instances
    */
  def parseBrokerList(brokerList: String): Seq[Broker] = {
    brokerList.split("[,]").toList map {
      _.split("[:]").toList match {
        case host :: Nil => Broker(host, 9091)
        case host :: port :: Nil => Broker(host, port.toInt)
        case args =>
          throw new IllegalArgumentException(s"Illegal host definition - '${args.mkString(", ")}'")
      }
    }
  }

}