package com.ldaniels528.trifecta.rest

import akka.actor.{ActorLogging, Actor}
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.WebSocketActor._
import org.mashupbots.socko.events.WebSocketFrameEvent
import org.slf4j.LoggerFactory

/**
 * Web Socket Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebSocketActor(embeddedWebServer: EmbeddedWebServer) extends Actor with ActorLogging {
  override def receive = {
    case event: WebSocketFrameEvent =>
      writeWebSocketResponse(event)
      context.stop(self)

    case message =>
      log.warning(s"received unknown message of type: $message")
      unhandled(message)
      context.stop(self)
  }

  /**
   * Processing incoming web socket events
   */
  private def writeWebSocketResponse(event: WebSocketFrameEvent) {
    val webSocketId = event.webSocketId

    // was it a binary message?
    if (event.isBinary) {
      val message = event.readBinary()
      log.info(s"[binary]: $message")
    }

    // was it a text-based message?
    else if (event.isText) {
      val message = event.readText()
      log.info(s"[text]: $message")

      message match {
        case js if js.contains("startMessageSampling") =>
          val request = JsonHelper.transform[StartSamplingRequest](js)
          embeddedWebServer.startMessageSampling(webSocketId, request.topic, request.partitions)

        case js if js.contains("stopMessageSampling") =>
          val request = JsonHelper.transform[StopSamplingRequest](js)
          embeddedWebServer.stopMessageSampling(webSocketId, request.topic)
      }
    }

    else {
      log.error(s"Unknown message type: $event")
    }
  }

}

/**
 * Web Socket Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object WebSocketActor {

  case class StartSamplingRequest(action: String, topic: String, partitions: Seq[Long])

  case class StopSamplingRequest(action: String, topic: String)

}
