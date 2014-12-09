package com.ldaniels528.trifecta.rest

import akka.actor.Actor
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.PushEventActor._
import org.mashupbots.socko.webserver.WebServer
import org.slf4j.LoggerFactory

/**
 * Push Event Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class PushEventActor(webServer: WebServer, facade: KafkaRestFacade) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def receive = {
    case PushConsumers => pushConsumerUpdateEvents()
    case PushTopics => pushTopicUpdateEvents()
    case message =>
      logger.info(s"received unknown message of type: $message")
      unhandled(message)
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushConsumerUpdateEvents() {
    val deltas = facade.getConsumerDeltas
    if (deltas.nonEmpty) {
      logger.info(s"Retrieved ${deltas.length} consumer(s)...")
      webServer.webSocketConnections.writeText(JsonHelper.makeCompact(deltas))
    }
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushTopicUpdateEvents() {
    val deltas = facade.getTopicDeltas
    if (deltas.nonEmpty) {
      logger.info(s"Retrieved ${deltas.length} topic(s)...")
      webServer.webSocketConnections.writeText(JsonHelper.makeCompact(deltas))
    }
  }

}

/**
 * Push Event Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object PushEventActor {

  case object PushConsumers

  case object PushTopics

}
