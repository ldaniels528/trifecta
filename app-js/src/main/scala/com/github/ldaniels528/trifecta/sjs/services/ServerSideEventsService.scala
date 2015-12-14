package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.scalascript._
import com.github.ldaniels528.scalascript.core.Http
import com.github.ldaniels528.trifecta.sjs.RootScope
import com.github.ldaniels528.trifecta.sjs.models.{OperationResult, SamplingRequest, SubmittedResult}
import org.scalajs.dom._
import org.scalajs.dom.raw.EventSource

import scala.scalajs.js
import scala.scalajs.js.JSON

/**
  * Server Side Events Service
  * @author lawrence.daniels@gmail.com
  */
class ServerSideEventsService($rootScope: RootScope, $http: Http) extends Service {

  /**
    * Establishes a SSE connection
    */
  def connect() {
    console.info(s"User is connecting to the SSE channel...")
    val eventSource = new EventSource("/api/sse/connect")

    // define the message event handler
    eventSource.addEventListener("message", handleMessage, useCapture = false)

    // define the error event handler
    eventSource.addEventListener("error", handleError, useCapture = false)
  }

  /**
    * Retrieves the current message sampling session
    */
  def getSamplingSession = {
    $http.get[SubmittedResult](url = "/api/sse/sampling")
  }

  /**
    * Starts message sampling
    */
  def startSampling(topic: String, partitionOffsets: js.Array[Int]) = {
    $http.put[SubmittedResult](url = "/api/sse/sampling", data = SamplingRequest(topic, partitionOffsets))
  }

  /**
    * Stops message sampling
    */
  def stopSampling(sessionId: String) = {
    $http.delete[OperationResult](url = s"/api/sse/sampling/$sessionId")
  }

  /**
    * Handles the incoming SSE message event
    */
  private def handleMessage: js.Function1[SSEMessageEvent, Any] = (event: SSEMessageEvent) => {
    event.data foreach { evtData =>
      JSON.parse(evtData).asInstanceOf[ReactiveMessage] match {
        // is it an action command?
        case evt if evt.`type`.isDefined =>
          for (action <- evt.`type`; message <- evt.message) $rootScope.$broadcast(action, message)

        // unrecognized message
        case message =>
          console.error("Message does not contain a 'type' key")
          console.info(s"Event message => ${angular.toJson(message, pretty = true)}")
      }
    }
  }

  /**
    * Handles failures
    */
  private def handleError: js.Function1[Event, Unit] = (event: Event) => {
    console.error(s"SSE error: ${angular.toJson(event)}")
  }

}

/**
  * Server Side Events Service Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ServerSideEventsService {
  val CONSUMER_DELTA = "consumer_deltas"
  val MESSAGE_SAMPLE = "sample"
  val TOPIC_DELTA = "topic_deltas"

}

/**
  * Represents a SSE Message Event
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SSEMessageEvent extends js.Object {
  var data: js.UndefOr[String] = js.native

}

/**
  * Represents a Reactive Message
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ReactiveMessage extends js.Object {
  var `type`: js.UndefOr[String] = js.native
  var message: js.UndefOr[js.Object] = js.native

}
