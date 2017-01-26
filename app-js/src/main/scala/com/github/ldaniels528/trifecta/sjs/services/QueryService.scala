package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.models.Query.QueryResultSet
import com.github.ldaniels528.trifecta.sjs.models.{Message, Query}
import com.github.ldaniels528.trifecta.sjs.services.QueryService._
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js
import scala.scalajs.js.Array
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Query Service
  * @author lawrence.daniels@gmail.com
  */
class QueryService($http: Http) extends Service {

  def executeQuery(queryString: String): HttpResponse[QueryResultSet] = {
    $http.post[QueryResultSet](
      url = "/api/query/many",
      data = new QueryRequest(queryString),
      headers = js.Dictionary("Content-Type" -> "application/json"))
  }

  def findOne(topic: String, criteria: String): HttpResponse[Message] = {
    $http.get[Message](s"/api/query/one/${topic.encode}/${criteria.encode}")
  }

  def getQueries: HttpResponse[Array[Query]] = {
    $http.get[js.Array[Query]]("/api/queries")
  }

  def getQueriesByTopic(topic: String): HttpResponse[Array[Query]] = {
    $http.get[js.Array[Query]](s"/api/query/${topic.encode}")
  }

}

/**
  * Query Service Companion
  * @author lawrence.daniels@gmail.com
  */
object QueryService {

  @ScalaJSDefined
  class QueryRequest(val queryString: String) extends js.Object

  @ScalaJSDefined
  class SaveQueryRequest(val name: String, val topic: String, val queryString: String) extends js.Object

}