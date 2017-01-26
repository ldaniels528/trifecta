package com.github.ldaniels528.trifecta.sjs.services

import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http._
import io.scalajs.dom.html.browser.encodeURI
import com.github.ldaniels528.trifecta.sjs.models._

import scala.scalajs.js
import scala.scalajs.js.Array

/**
  * Topic Service
  * @author lawrence.daniels@gmail.com
  */
class TopicService($http: Http) extends Service {

  def getDetailedTopic(topic: String): HttpResponse[Array[TopicDetails]] = {
    $http.get[js.Array[TopicDetails]](s"/api/topics/details/$topic")
  }

  def getDetailedTopics: HttpResponse[Array[TopicDetails]] = {
    $http.get[js.Array[TopicDetails]]("/api/topics/details")
  }

  def getTopic(topic: String): HttpResponse[Array[TopicDetails]] = {
    $http.get[js.Array[TopicDetails]](s"/api/topic/$topic")
  }

  def getTopics: HttpResponse[Array[TopicDetails]] = {
    $http.get[js.Array[TopicDetails]]("/api/topics")
  }

}
