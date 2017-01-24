package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.models._
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js

/**
  * Consumer Group Service
  * @author lawrence.daniels@gmail.com
  */
class ConsumerGroupService($http: Http)  extends Service {

  /**
    * Retrieves the consumer group skeletons for the given topic
    * @return a promise of an array of [[Consumer]]s
    */
  def getConsumer(groupId: String): HttpResponse[Consumer] = {
    $http.get[Consumer](s"/api/consumer/${groupId.encode}")
  }

  /**
    * Retrieves the consumer group offsets for the given group ID
    * @return a promise of an array of [[ConsumerOffset consumer offsets]]
    */
  def getConsumerOffsets(groupId: String): HttpResponse[js.Array[ConsumerOffset]] = {
    $http.get[js.Array[ConsumerOffset]](s"/api/consumer/${groupId.encode}/offsets")
  }

  /**
    * Retrieves the consumer group skeletons for the given topic
    * @return a promise of an array of [[Consumer]]s
    */
  def getConsumers: HttpResponse[js.Array[Consumer]] = {
    $http.get[js.Array[Consumer]]("/api/consumers")
  }

}
