package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.meansjs.angularjs.Service
import com.github.ldaniels528.meansjs.angularjs.http._
import com.github.ldaniels528.meansjs.core.browser.encodeURI
import com.github.ldaniels528.meansjs.util.ScalaJsHelper._
import com.github.ldaniels528.trifecta.sjs.models._

import scala.concurrent.ExecutionContext
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
  def getBrokerGroups(implicit ec: ExecutionContext) = {
    $http.get[js.Array[BrokerGroup]]("/api/brokers/grouped") map (_.data)
  }

  /**
    * Retrieves the array of Kafka brokers
    * @return a promise of an array of [[Broker brokers]]
    */
  def getBrokers(implicit ec: ExecutionContext) = {
    $http.get[js.Array[Broker]]("/api/brokers") map (_.data)
  }

  /**
    * Retrieves the replica brokers for a given topic
    * @param topic a given topic (e.g. "stocks.nyse")
    * @return a promise of an array of [[ReplicaBroker replica brokers]]
    */
  def getReplicas(topic: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[ReplicaGroup]](s"/api/replicas/$topic") map (_.data)
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Consumer Groups
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * Retrieves the consumer groups for the given topic
    * @param topic the given topic
    * @return a promise of an array of [[ConsumerGroup consumer groups]]
    */
  def getConsumerGroups(topic: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[ConsumerGroup]](s"/api/consumers/topic/${encodeURI(topic)}") map (_.data)
  }

  /**
    * Retrieves all consumers
    * @return a promise of an array of [[Consumer consumer]]
    */
  def getConsumers(implicit ec: ExecutionContext) = {
    $http.get[js.Array[Consumer]]("/api/consumers/details") map (_.data)
  }

  /////////////////////////////////////////////////////////////////////////////////
  //        Topics
  /////////////////////////////////////////////////////////////////////////////////

  def getDetailedTopic(topic: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[TopicDetails]](s"/api/topics/details/$topic") map (_.data)
  }

  def getDetailedTopics(implicit ec: ExecutionContext) = {
    $http.get[js.Array[TopicDetails]]("/api/topics/details") map (_.data)
  }

  def getTopic(topic: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[TopicDetails]](s"/api/topic/$topic") map (_.data)
  }

  def getTopics(implicit ec: ExecutionContext) = {
    $http.get[js.Array[TopicDetails]]("/api/topics") map (_.data)
  }

}
