package com.ldaniels528.trifecta.rest

import com.ldaniels528.trifecta.util.OptionHelper._
import java.io.File

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.TxConfig._
import com.ldaniels528.trifecta.rest.EmbeddedWebServer.TxQuery
import com.ldaniels528.trifecta.util.StringHelper._

import scala.io.Source
import scala.util.Try

/**
 * Trifecta Web Configuration
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TxWebConfig {

  /**
   * Initializes the configuration
   */
  def init(config: TxConfig): Unit = {
    Seq(decoderDirectory, queriesDirectory) foreach { directory =>
      if (!directory.exists()) Try(directory.mkdirs())
    }
  }

  /**
   * Returns the location of the queries directory
   * @return the [[File]] representing the location of the queries directory
   */
  def queriesDirectory: File = new File(trifectaPrefs, "queries")

  /**
   * Trifecta Configuration Extensions
   * @param config the given [[TxConfig]]
   */
  implicit class TxConfigExtensions(val config: TxConfig) extends AnyVal {

    def getQueries: Option[Seq[TxQuery]] = {
      def removeExtension(name: String) = name.lastIndexOptionOf(".bdql") ?? name.lastIndexOptionOf(".kql") match {
        case Some(index) => name.substring(0, index)
        case None => name
      }

      Option(queriesDirectory.listFiles) map { queriesFiles =>
        queriesFiles map { file =>
          val name = removeExtension(file.getName)
          TxQuery(name, Source.fromFile(file).getLines().mkString("\n"), file.exists(), file.lastModified())
        }
      }
    }

    /**
     * Returns the push interval (in seconds) for topic changes
     * @return the interval
     */
    def topicPushInterval: Int = config.getOrElse("trifecta.web.push.interval.topic", "15").toInt

    /**
     * Returns the push interval (in seconds) for consumer offset changes
     * @return the interval
     */
    def consumerPushInterval: Int = config.getOrElse("trifecta.web.push.interval.consumer", "15").toInt

    /**
     * Returns the web actor execution concurrency
     * @return the web actor execution concurrency
     */
    def webActorConcurrency: Int = config.getOrElse("trifecta.web.actor.concurrency", "10").toInt

    /**
     * Returns the embedded web server host/IP
     * @return the embedded web server host/IP
     */
    def webHost: String = config.getOrElse("trifecta.web.host", "localhost")

    /**
     * Returns the embedded web server port
     * @return the embedded web server port
     */
    def webPort: Int = config.getOrElse("trifecta.web.port", "8888").toInt

  }

}
