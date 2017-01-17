package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.models.{ConsumerRedux, _}
import org.scalajs.angularjs.Service
import org.scalajs.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js

/**
  * Consumer Group Service
  * @author lawrence.daniels@gmail.com
  */
class ConsumerGroupService($http: Http)  extends Service {

  /**
    * Retrieves the consumer group skeletons for the given topic
    * @return a promise of an array of [[ConsumerRedux]]s
    */
  def getConsumer(groupId: String): HttpResponse[ConsumerRedux] = {
    $http.get[ConsumerRedux](s"/api/consumer/${groupId.encode}")
  }

  /**
    * Retrieves the consumer groups for the given topic
    * @param topic the given topic
    * @return a promise of an array of [[ConsumerGroup consumer groups]]
    */
  def getConsumerGroups(topic: String): HttpResponse[js.Array[ConsumerGroup]] = {
    $http.get[js.Array[ConsumerGroup]](s"/api/consumers/topic/${topic.encode}")
  }

  /**
    * Retrieves all consumers
    * @return a promise of an array of [[Consumer consumer]]
    */
  def getConsumers: HttpResponse[js.Array[Consumer]] = {
    $http.get[js.Array[Consumer]]("/api/consumers/details")
  }

  /**
    * Retrieves the consumer group skeletons for the given topic
    * @return a promise of an array of [[ConsumerRedux]]s
    */
  def getConsumersLite: HttpResponse[js.Array[ConsumerRedux]] = {
    $http.get[js.Array[ConsumerRedux]]("/api/consumers/lite")
  }

  /**
    * Retrieves the consumer group offsets for the given group ID
    * @return a promise of an array of [[ConsumerOffset consumer offsets]]
    */
  def getConsumerOffsets(groupId: String): HttpResponse[js.Array[ConsumerOffset]] = {
    $http.get[js.Array[ConsumerOffset]](s"/api/consumer/${groupId.encode}/offsets")
  }

}
