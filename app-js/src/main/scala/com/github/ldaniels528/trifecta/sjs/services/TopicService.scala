package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.meansjs.angularjs.Service
import com.github.ldaniels528.meansjs.angularjs.http._
import com.github.ldaniels528.meansjs.core.browser.encodeURI
import com.github.ldaniels528.trifecta.sjs.models._

import scala.scalajs.js

/**
  * Topic Service
  * @author lawrence.daniels@gmail.com
  */
class TopicService($http: Http) extends Service {

  /////////////////////////////////////////////////////////////////////////////////
  //        Brokers & Replicas
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Retrieves the array of Kafka brokers
    * @return a promise of an array of [[BrokerGroup grouped brokers]]
    */
  def getBrokerGroups = {
    $http.get[js.Array[BrokerGroup]]("/api/brokers/grouped")
  }

  /**
    * Retrieves the array of Kafka brokers
    * @return a promise of an array of [[Broker brokers]]
    */
  def getBrokers = {
    $http.get[js.Array[Broker]]("/api/brokers")
  }

  /**
    * Retrieves the replica brokers for a given topic
    * @param topic a given topic (e.g. "stocks.nyse")
    * @return a promise of an array of [[ReplicaBroker replica brokers]]
    */
  def getReplicas(topic: String) = {
    $http.get[js.Array[ReplicaGroup]](s"/api/replicas/$topic")
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Consumer Groups
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Retrieves the consumer groups for the given topic
    * @param topic the given topic
    * @return a promise of an array of [[ConsumerGroup consumer groups]]
    */
  def getConsumerGroups(topic: String) = {
    $http.get[js.Array[ConsumerGroup]](s"/api/consumers/topic/${encodeURI(topic)}")
  }

  /**
    * Retrieves all consumers
    * @return a promise of an array of [[Consumer consumer]]
    */
  def getConsumers = {
    $http.get[js.Array[Consumer]]("/api/consumers/details")
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Topics
  /////////////////////////////////////////////////////////////////////////////////

  def getDetailedTopic(topic: String) = {
    $http.get[js.Array[TopicDetails]](s"/api/topics/details/$topic")
  }

  def getDetailedTopics = {
    $http.get[js.Array[TopicDetails]]("/api/topics/details")
  }

  def getTopic(topic: String) = {
    $http.get[js.Array[TopicDetails]](s"/api/topic/$topic")
  }

  def getTopics = {
    $http.get[js.Array[TopicDetails]]("/api/topics")
  }

}
