package com.github.ldaniels528.trifecta.rest

import com.github.ldaniels528.trifecta.TxConfig

/**
 * Trifecta Web Configuration
 * @author lawrence.daniels@gmail.com
 */
object TxWebConfig {

  /**
   * Trifecta Configuration Extensions
   * @param config the given [[TxConfig]]
   */
  implicit class TxConfigExtensions(val config: TxConfig) extends AnyVal {

    /**
     * Returns the push interval (in seconds) for consumer offset changes
     * @return the interval in seconds
     */
    def consumerPushInterval: Int = config.getOrElse("trifecta.web.push.interval.consumer", "15").toInt

    /**
     * Returns the push interval (in seconds) for topic changes
     * @return the interval in seconds
     */
    def topicPushInterval: Int = config.getOrElse("trifecta.web.push.interval.topic", "15").toInt

    /**
     * Returns the push interval (in seconds) for message sampling
     * @return the interval in seconds
     */
    def samplingPushInterval: Int = config.getOrElse("trifecta.web.push.interval.sampling", "2").toInt

    /**
     * Returns the web actor execution concurrency
     * @return the web actor execution concurrency
     */
    def webActorConcurrency: Int = config.getOrElse("trifecta.web.actor.concurrency", "10").toInt

    /**
     * Returns the embedded web server host/IP
     * @return the embedded web server host/IP
     */
    def webHost: String = config.getOrElse("trifecta.web.host", "0.0.0.0")

    /**
     * Returns the embedded web server port
     * @return the embedded web server port
     */
    def webPort: Int = config.getOrElse("trifecta.web.port", "8888").toInt

  }

}
