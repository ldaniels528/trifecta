package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.RootScope
import com.github.ldaniels528.trifecta.sjs.models._
import com.github.ldaniels528.trifecta.sjs.services.ServerSideEventsService._
import org.scalajs.angularjs._
import org.scalajs.angularjs.http.{Http, HttpResponse}
import org.scalajs.dom.{Event, EventSource}

import scala.scalajs.js
import scala.scalajs.js.JSON

/**
  * Server Side Events Service
  * @author lawrence.daniels@gmail.com
  */
class ServerSideEventsService($rootScope: RootScope, $http: Http, $log: Log) extends Service {

  /**
    * Establishes a SSE connection
    */
  def connect() {
    $log.info("User is connecting to the SSE channel...")
    val eventSource = new EventSource("/api/sse/connect")

    // define the message event handler
    eventSource.addEventListener("message", handleMessage, useCapture = false)

    // define the error event handler
    eventSource.addEventListener("error", handleError, useCapture = false)
  }

  /**
    * Retrieves the current message sampling session
    */
  def getSamplingSession: HttpResponse[SubmittedResult] = {
    $http.get[SubmittedResult](url = "/api/sse/sampling")
  }

  /**
    * Starts message sampling
    */
  def startSampling(topic: String, partitionOffsets: js.Array[Int]): HttpResponse[SubmittedResult] = {
    $http.put[SubmittedResult](url = "/api/sse/sampling", data = SamplingRequest(topic, partitionOffsets))
  }

  /**
    * Stops message sampling
    */
  def stopSampling(sessionId: String): HttpResponse[OperationResult] = {
    $http.delete[OperationResult](url = s"/api/sse/sampling/$sessionId")
  }

  /**
    * Handles the incoming SSE message event
    */
  def handleMessage: js.Function1[SSEMessageEvent, Any] = (event: SSEMessageEvent) => {
    event.data foreach { evtData =>
      JSON.parse(evtData).asInstanceOf[ReactiveMessage] match {
        // is it an action command?
        case evt if evt.`type`.nonEmpty =>
          for (action <- evt.`type`; message <- evt.message) $rootScope.$broadcast(action, message)

        // unrecognized message
        case message =>
          $log.error("Message does not contain a 'type' key")
          $log.info(s"Event message => ${angular.toJson(message, pretty = true)}")
      }
    }
  }

  /**
    * Handles failures
    */
  private def handleError: js.Function1[Event, Unit] = (event: Event) => {
    $log.error(s"SSE error: ${angular.toJson(event)}")
  }

}

/**
  * Server Side Events Service Companion
  * @author lawrence.daniels@gmail.com
  */
object ServerSideEventsService {

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

}