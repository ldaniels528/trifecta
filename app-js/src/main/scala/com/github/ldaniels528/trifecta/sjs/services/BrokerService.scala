package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.models.{Broker, BrokerGroup, ReplicaBroker, ReplicaGroup}
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js
import scala.scalajs.js.Array

/**
  * Broker Service
  * @author lawrence.daniels@gmail.com
  */
class BrokerService($http: Http)  extends Service {

  /**
    * Retrieves the array of Kafka brokers
    * @return a promise of an array of [[BrokerGroup grouped brokers]]
    */
  def getBrokerGroups: HttpResponse[Array[BrokerGroup]] = {
    $http.get[js.Array[BrokerGroup]]("/api/brokers/grouped")
  }

  /**
    * Retrieves the array of Kafka brokers
    * @return a promise of an array of [[Broker brokers]]
    */
  def getBrokers: HttpResponse[Array[Broker]] = {
    $http.get[js.Array[Broker]]("/api/brokers")
  }

  /**
    * Retrieves the replica brokers for a given topic
    * @param topic a given topic (e.g. "stocks.nyse")
    * @return a promise of an array of [[ReplicaBroker replica brokers]]
    */
  def getReplicas(topic: String): HttpResponse[Array[ReplicaGroup]] = {
    $http.get[js.Array[ReplicaGroup]](s"/api/replicas/$topic")
  }

}
