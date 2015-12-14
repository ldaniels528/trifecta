package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import scala.scalajs.js

/**
  * Represents Topic Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TopicDetails extends js.Object {
  var topic: String = js.native
  var partitions: js.Array[PartitionDetails] = js.native
  var leader: js.Array[Broker] = js.native
  var replicas: js.Array[ReplicaGroup] = js.native
  var totalMessages: Int = js.native

  // ui-specific properties
  var expanded: js.UndefOr[Boolean] = js.native
  var loading: js.UndefOr[Boolean] = js.native
  var loadingConsumers: js.UndefOr[Boolean] = js.native
  var queriesExpanded: js.UndefOr[Boolean] = js.native
  var replicaExpanded: js.UndefOr[Boolean] = js.native
  var savedQueries: js.UndefOr[js.Array[Query]] = js.native
  var updatingTopics: js.UndefOr[Int] = js.native

}

/**
  * Topic Details Companion Object
  * @author lawrence.daniels@gmail.com
  */
object TopicDetails {

  /**
    * Topic Details Enrichment
    * @author lawrence.daniels@gmail.com
    */
  implicit class TopicDetailsEnrichment(val topic: TopicDetails) extends AnyVal {

    def makeClone = {
      val cloned = makeNew[TopicDetails]
      cloned.topic = topic.topic
      cloned.partitions = topic.partitions
      cloned.leader = topic.leader
      cloned.replicas = topic.replicas
      cloned.totalMessages = topic.totalMessages

      cloned.expanded = topic.expanded
      cloned.queriesExpanded = topic.queriesExpanded
      cloned.replicaExpanded = topic.replicaExpanded
      cloned.savedQueries = topic.savedQueries
      cloned
    }

    def findPartition(partitionId: Int) = topic.partitions.find(_.partition == partitionId)

  }

}