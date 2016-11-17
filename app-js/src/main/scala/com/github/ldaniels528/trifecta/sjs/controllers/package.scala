package com.github.ldaniels528.trifecta.sjs

import com.github.ldaniels528.trifecta.sjs.models.{ConsumerDelta, Message, PartitionDelta, ReferenceData}
import org.scalajs.angularjs.Scope
import org.scalajs.dom
import org.scalajs.dom.browser.console

import scala.scalajs.js

/**
  * controllers package object
  * @author lawrence.daniels@gmail.com
  */
package object controllers {
  private val CONSUMER_DELTA = "consumer_deltas"
  private val MESSAGE_SAMPLE = "sample"
  private val REFERENCE_DATA_LOADED = "reference_data_loaded"
  private val TOPIC_DELTA = "topic_deltas"

  type CancellablePromise = js.Promise[js.Any]

  /**
    * Application Data Events
    * @param scope the given [[Scope scope]]
    */
  final implicit class ApplicationEvents(val scope: Scope) extends AnyVal {

    @inline
    def broadcastReferenceDataLoaded(data: ReferenceData) = {
      console.log(s"Broadcasting '$REFERENCE_DATA_LOADED' event...")
      scope.$broadcast(REFERENCE_DATA_LOADED, data)
    }

    @inline
    def onReferenceDataLoaded(f: ReferenceData => Any) = {
      scope.$on(REFERENCE_DATA_LOADED, (event: dom.Event, data: ReferenceData) => f(data))
    }

    @inline
    def onConsumerDeltas(f: js.Array[ConsumerDelta] => Any) = {
      scope.$on(CONSUMER_DELTA, (event: dom.Event, deltas: js.Array[ConsumerDelta]) => f(deltas))
    }

    @inline
    def onMessageSample(f: Message => Any) = {
      scope.$on(MESSAGE_SAMPLE, (event: dom.Event, message: Message) => f(message))
    }

    @inline
    def onTopicDeltas(f: js.Array[PartitionDelta] => Any) = {
      scope.$on(TOPIC_DELTA, (event: dom.Event, deltas: js.Array[PartitionDelta]) => f(deltas))
    }

  }

}
