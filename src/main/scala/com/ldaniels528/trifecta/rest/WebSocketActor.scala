package com.ldaniels528.trifecta.rest

import akka.actor.Actor
import org.mashupbots.socko.events.WebSocketFrameEvent
import org.slf4j.LoggerFactory

/**
 * Web Socket Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebSocketActor(facade: KafkaRestFacade) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def receive = {
    case event: WebSocketFrameEvent => writeWebSocketResponse(event)
    case message =>
      logger.warn(s"received unknown message of type: $message")
      unhandled(message)
  }

  /**
   * Processing incoming web socket events
   */
  private def writeWebSocketResponse(event: WebSocketFrameEvent) {
    logger.info(s"TextWebSocketFrame: ${event.readText()}")
    // TODO incoming events will be handled here
  }

}
