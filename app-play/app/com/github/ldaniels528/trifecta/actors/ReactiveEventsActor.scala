package com.github.ldaniels528.trifecta.actors

import akka.actor.{Actor, ActorLogging, Cancellable}
import com.github.ldaniels528.trifecta.actors.ReactiveEventsActor._
import com.github.ldaniels528.trifecta.controllers.WebConfig
import com.github.ldaniels528.trifecta.models._
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.json.Json

import scala.concurrent.duration._

/**
  * Represents a Reactive Events Actor
  * @author lawrence.daniels@gmail.com
  */
class ReactiveEventsActor() extends Actor with ActorLogging {

  override def receive = {
    case message: MessageSamplingStartRequest =>
      val caller = sender()
      caller ! startSampling(message)

    case StreamingConsumerUpdateRequest(frequency) =>
      val caller = sender()
      caller ! startStreamingConsumerUpdates(frequency.seconds)

    case StreamingTopicUpdateRequest(frequency) =>
      val caller = sender()
      caller ! startStreamingTopicUpdates(frequency.seconds)

    case message =>
      log.warning(s"Unhandled message '$message' (${Option(message).map(_.getClass.getName).orNull})")
      unhandled(message)
  }

  /**
    * Starts the process of sampling messages
    * @param request the given [[MessageSamplingStartRequest sampling start request]]
    * @return the sampling session ID
    */
  private def startSampling(request: MessageSamplingStartRequest) = {
    val cursor = WebConfig.facade.createSamplingCursor(request)
    val promise = context.system.scheduler.schedule(0.second, 3.second)(
      WebConfig.facade.findNext(cursor) foreach { message =>
        SSE ! SSEMessage(`type` = MESSAGE_SAMPLE, message = Json.toJson(message))
      })
    SamplingSession(cursor, promise)
  }


  private def startStreamingConsumerUpdates(frequency: FiniteDuration) = {
    log.info("Preparing streaming consumer updates...")
    context.system.scheduler.schedule(frequency, frequency) {
      val deltas = WebConfig.facade.getConsumerDeltas
      if (deltas.nonEmpty) SSE ! SSEMessage(`type` = CONSUMER_DELTA, message = Json.toJson(deltas))
    }
  }

  private def startStreamingTopicUpdates(frequency: FiniteDuration) = {
    log.info("Preparing streaming topic updates...")
    context.system.scheduler.schedule(frequency, frequency) {
      val deltas = WebConfig.facade.getTopicDeltas
      if (deltas.nonEmpty) SSE ! SSEMessage(`type` = TOPIC_DELTA, message = Json.toJson(deltas))
    }
  }

}

/**
  * Reactive Events Actor
  * @author lawrence.daniels@gmail.com
  */
object ReactiveEventsActor {
  val CONSUMER_DELTA = "consumer_deltas"
  val MESSAGE_SAMPLE = "sample"
  val TOPIC_DELTA = "topic_deltas"

  /**
    * Represents a message sampling session
    * @param cursor the given [[SamplingCursor sampling cursor]]
    * @param promise the given [[Cancellable sampling promise]]
    */
  case class SamplingSession(cursor: SamplingCursor, promise: Cancellable)

}