package com.github.ldaniels528.trifecta.sjs

import com.github.ldaniels528.trifecta.AppConstants._
import com.github.ldaniels528.trifecta.sjs.models._
import org.scalajs.angularjs.Scope
import org.scalajs.dom
import org.scalajs.dom.browser.console

import scala.scalajs.js

/**
  * controllers package object
  * @author lawrence.daniels@gmail.com
  */
package object controllers {
  private val CONSUMERS_LOADED = "consumers_loaded"
  private val REFERENCE_DATA_LOADED = "reference_data_loaded"
  private val TOPICS_LOADED = "topics_loaded"

  type CancellablePromise = js.Promise[js.Any]

  /**
    * Application Data Events
    * @param scope the given [[Scope scope]]
    */
  final implicit class ApplicationEvents(val scope: Scope) extends AnyVal {

    ///////////////////////////////////////////////////////////////////////////////
    //    Reference Data
    ///////////////////////////////////////////////////////////////////////////////

    @inline
    def broadcastReferenceDataLoaded(data: ReferenceData) {
      console.log(s"Broadcasting '$REFERENCE_DATA_LOADED' event...")
      scope.$broadcast(REFERENCE_DATA_LOADED, data)
    }

    @inline
    def onReferenceDataLoaded(f: ReferenceData => Any) {
      scope.$on(REFERENCE_DATA_LOADED, (event: dom.Event, data: ReferenceData) => f(data))
    }

    ///////////////////////////////////////////////////////////////////////////////
    //    Consumers
    ///////////////////////////////////////////////////////////////////////////////

    @inline
    def broadcastConsumersLoaded(consumers: js.Array[Consumer]) {
      console.log(s"Broadcasting '$CONSUMERS_LOADED' event...")
      scope.$broadcast(CONSUMERS_LOADED, consumers)
    }

    @inline
    def onConsumerDeltas(f: js.Array[ConsumerDelta] => Any) {
      scope.$on(CONSUMER_DELTAS, (event: dom.Event, deltas: js.Array[ConsumerDelta]) => f(deltas))
    }

    @inline
    def onConsumersLoaded(f: js.Array[Consumer] => Any) {
      scope.$on(CONSUMERS_LOADED, (event: dom.Event, consumers: js.Array[Consumer]) => f(consumers))
    }

    ///////////////////////////////////////////////////////////////////////////////
    //    Sampling / SSE
    ///////////////////////////////////////////////////////////////////////////////

    @inline
    def onMessageSample(f: Message => Any) {
      scope.$on(MESSAGE_SAMPLE, (event: dom.Event, message: Message) => f(message))
    }

    ///////////////////////////////////////////////////////////////////////////////
    //    Topics
    ///////////////////////////////////////////////////////////////////////////////

    @inline
    def broadcastTopicsLoaded(topics: js.Array[TopicDetails]) {
      console.log(s"Broadcasting '$TOPICS_LOADED' event...")
      scope.$broadcast(TOPICS_LOADED, topics)
    }

    @inline
    def onTopicDeltas(f: js.Array[PartitionDelta] => Any) {
      scope.$on(TOPIC_DELTAS, (event: dom.Event, deltas: js.Array[PartitionDelta]) => f(deltas))
    }

    @inline
    def onTopicsLoaded(f: js.Array[TopicDetails] => Any) {
      scope.$on(CONSUMERS_LOADED, (event: dom.Event, topics: js.Array[TopicDetails]) => f(topics))
    }

  }

}
