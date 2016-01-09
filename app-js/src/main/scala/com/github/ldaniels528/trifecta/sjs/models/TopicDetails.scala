package com.github.ldaniels528.trifecta.sjs.models

import com.github.ldaniels528.scalascript.util.ScalaJsHelper._
import org.scalajs.dom.console

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
  implicit class TopicDetailsEnrichment(val details: TopicDetails) extends AnyVal {

    def apply(partitionId: Int) = details.partitions.find(_.partition == partitionId)

    def copy(topic: js.UndefOr[String] = js.undefined,
             partitions: js.UndefOr[js.Array[PartitionDetails]] = js.undefined,
             leader: js.UndefOr[js.Array[Broker]] = js.undefined,
             replicas: js.UndefOr[js.Array[ReplicaGroup]] = js.undefined,
             totalMessages: js.UndefOr[Int] = js.undefined,
             expanded: js.UndefOr[Boolean] = js.undefined,
             loading: js.UndefOr[Boolean] = js.undefined,
             loadingConsumers: js.UndefOr[Boolean] = js.undefined,
             queriesExpanded: js.UndefOr[Boolean] = js.undefined,
             replicaExpanded: js.UndefOr[Boolean] = js.undefined,
             savedQueries: js.UndefOr[js.Array[Query]] = js.undefined,
             updatingTopics: js.UndefOr[Int] = js.undefined) = {
      val cloned = makeNew[TopicDetails]
      cloned.topic = topic getOrElse details.topic
      cloned.partitions = partitions getOrElse details.partitions
      cloned.leader = leader getOrElse details.leader
      cloned.replicas = replicas getOrElse details.replicas
      cloned.totalMessages = totalMessages getOrElse details.totalMessages
      cloned.expanded = expanded ?? details.expanded
      cloned.loading = loading ?? details.loading
      cloned.loadingConsumers = loadingConsumers ?? details.loadingConsumers
      cloned.queriesExpanded = queriesExpanded ?? details.queriesExpanded
      cloned.replicaExpanded = replicaExpanded ?? details.replicaExpanded
      cloned.savedQueries = savedQueries ?? details.savedQueries
      cloned.updatingTopics = updatingTopics ?? details.updatingTopics
      cloned
    }

    def replace(delta: PartitionDelta) {
      details.partitions.indexWhere(p => p.partition ?== delta.partition) match {
        case -1 =>
          details.partitions.push(PartitionDetails(delta))
        case index =>
          details.partitions(index) = details.partitions(index).copy(
            startOffset = delta.startOffset,
            endOffset = delta.endOffset,
            messages = delta.messages,
            totalMessages = delta.totalMessages
          )
      }
    }

    def replace(message: Message) {
      for {
        partitionId <- message.partition
      } {
        details.partitions.indexWhere(_.partition.contains(partitionId)) match {
          case -1 =>
            console.warn(s"Partition $partitionId does not exist for topic ${details.topic}")
          case index =>
            details.partitions(index) = details.partitions(index).copy(offset = message.offset)
        }
      }
    }

  }

}