package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.controllers.QueryController.SavedResult
import com.github.ldaniels528.trifecta.sjs.models.{Message, Query, QueryResultSet, QueryRow}
import com.github.ldaniels528.trifecta.sjs.services.QueryService._
import org.scalajs.angularjs.Service
import org.scalajs.angularjs.http.Http

import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{global => g}
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Query Service
  * @author lawrence.daniels@gmail.com
  */
class QueryService($http: Http) extends Service {

  def executeQuery(name: String, queryString: String) = {
    $http.post[QueryResultSet](
      url = "/api/query/many",
      data = new QueryRequest(name, queryString),
      headers = js.Dictionary("Content-Type" -> "application/json"))
  }

  def findOne(topic: String, criteria: String) = {
    $http.get[Message](s"/api/query/one/${topic.encode}/${criteria.encode}")
  }

  def getQueries = {
    $http.get[js.Array[Query]]("/api/queries")
  }

  def getQueriesByTopic(topic: String) = {
    $http.get[js.Array[Query]](s"/api/query/${topic.encode}")
  }

  def saveQuery(name: String, topic: String, queryString: String) = {
    $http.post[Message](
      url = "/api/query",
      data = new SaveQueryRequest(name, topic, queryString),
      headers = js.Dictionary("Content-Type" -> "application/json"))
  }

}

/**
  * Query Service Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryService {

  @ScalaJSDefined
  class QueryRequest(val name: String, val queryString: String) extends js.Object

  @ScalaJSDefined
  class SaveQueryRequest(val name: String, val topic: String, val queryString: String) extends js.Object

}