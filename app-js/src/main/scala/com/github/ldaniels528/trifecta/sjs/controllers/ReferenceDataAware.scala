package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.Scope
import com.github.ldaniels528.trifecta.sjs.models._

import scala.scalajs.js

/**
  * Reference Data Aware
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ReferenceDataAware extends js.Object {
  self: Scope =>

  // properties
  var brokers: js.Array[BrokerGroup] = js.native
  var consumers: js.Array[Consumer] = js.native
  var replicas: js.Array[ReplicaGroup] = js.native
  var topic: js.UndefOr[TopicDetails] = js.native
  var topics: js.Array[TopicDetails] = js.native

  // consumer functions
  var getConsumers: js.Function0[js.Array[Consumer]] = js.native
  var getConsumersForTopic: js.Function1[js.UndefOr[String], js.Array[Consumer]] = js.native
  var getConsumersForIdAndTopic: js.Function2[js.UndefOr[String], js.UndefOr[String], js.UndefOr[js.Array[Consumer]]] = js.native

  // topic functions
  var findNonEmptyTopic: js.Function0[js.UndefOr[TopicDetails]] = js.native
  var findTopicByName: js.Function1[String, js.UndefOr[TopicDetails]] = js.native
  var getTopicIcon: js.Function2[js.UndefOr[TopicDetails], js.UndefOr[Boolean], js.UndefOr[String]] = js.native
  var getTopicIconSelection: js.Function1[js.UndefOr[Boolean], String] = js.native
  var getTopicNames: js.Function0[js.Array[String]] = js.native
  var getTopics: js.Function0[js.Array[TopicDetails]] = js.native

}

/**
  * Reference Data Aware Companion Object
  * @author lawrence.daniels@gmail.com
  */
object ReferenceDataAware {
  val REFERENCE_DATA_LOADED = "reference_data_loaded"

}