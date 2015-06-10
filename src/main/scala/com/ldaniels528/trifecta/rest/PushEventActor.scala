package com.ldaniels528.trifecta.rest

import akka.actor.{Actor, ActorLogging}
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.KafkaRestFacade.{SamplingCursor, TopicAndPartitions}
import com.ldaniels528.trifecta.rest.PushEventActor._

/**
 * Push Events Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class PushEventActor(embeddedWebServer: EmbeddedWebServer) extends Actor with ActorLogging {
  private val facade = embeddedWebServer.facade

  import context.dispatcher

  override def receive = {
    case PushConsumers =>
      pushConsumerUpdateEvents()
      context.stop(self)

    case PushMessage(webSocketId, topicAndPartitions) =>
      pushMessageSamplingEvent(webSocketId, topicAndPartitions)
      context.stop(self)

    case PushTopics =>
      pushTopicUpdateEvents()
      context.stop(self)

    case message =>
      log.warning(s"received unknown message of type: $message")
      unhandled(message)
      context.stop(self)
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushConsumerUpdateEvents() {
    facade.getConsumerDeltas.foreach { deltas =>
      if (deltas.nonEmpty) {
        val jsonMessage = JsonHelper.toJsonString(PushEvent(action = "consumerDeltas", deltas))
        embeddedWebServer.webServer.webSocketConnections.writeText(jsonMessage)
      }
    }
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushTopicUpdateEvents() {
    facade.getTopicDeltas.foreach { deltas =>
      if (deltas.nonEmpty) {
        val jsonMessage = JsonHelper.toJsonString(PushEvent(action = "topicDeltas", deltas))
        embeddedWebServer.webServer.webSocketConnections.writeText(jsonMessage)
      }
    }
  }

  /**
   * Pushes message sampling events for a given web socket client and topic
   * @param webSocketId the given web socket client ID
   * @param tap the given topic
   */
  private def pushMessageSamplingEvent(webSocketId: String, tap: TopicAndPartitions): Unit = {
    embeddedWebServer.getSession(webSocketId) foreach { session =>
      val cursorOffsets = facade.createSamplingCursorOffsets(tap)
      val cursor = session.cursors.getOrElseUpdate(webSocketId, SamplingCursor(tap.topic, cursorOffsets))
      facade.findNext(cursor) foreach (_ foreach { message =>
        val jsonMessage = JsonHelper.toJsonString(PushEvent(action = "sample", message))
        embeddedWebServer.webServer.webSocketConnections.writeText(jsonMessage, webSocketId)
      })
    }
  }

}

/**
 * Push Events Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object PushEventActor {

  case object PushConsumers

  case class PushEvent(action: String, data: AnyRef)

  case class PushMessage(webSocketId: String, topicAndPartitions: TopicAndPartitions)

  case object PushTopics

}
