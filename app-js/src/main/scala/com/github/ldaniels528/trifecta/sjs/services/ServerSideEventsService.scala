package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.meansjs.util.ScalaJsHelper._
import com.github.ldaniels528.meansjs.angularjs._
import com.github.ldaniels528.meansjs.angularjs.http.Http
import com.github.ldaniels528.meansjs.core.browser.console
import com.github.ldaniels528.trifecta.sjs.RootScope
import com.github.ldaniels528.trifecta.sjs.models._
import org.scalajs.dom.Event
import org.scalajs.dom.raw.EventSource

import scala.concurrent.ExecutionContext
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
  def getSamplingSession(implicit ec: ExecutionContext) = {
    $http.get[SubmittedResult](url = "/api/sse/sampling")map (_.data)
  }

  /**
    * Starts message sampling
    */
  def startSampling(topic: String, partitionOffsets: js.Array[Int])(implicit ec: ExecutionContext) = {
    $http.put[SubmittedResult](url = "/api/sse/sampling", data = SamplingRequest(topic, partitionOffsets))map (_.data)
  }

  /**
    * Stops message sampling
    */
  def stopSampling(sessionId: String)(implicit ec: ExecutionContext) = {
    $http.delete[OperationResult](url = s"/api/sse/sampling/$sessionId") map(_.data)
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
