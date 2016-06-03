package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.meansjs.angularjs.Service
import com.github.ldaniels528.meansjs.angularjs.http.Http
import com.github.ldaniels528.meansjs.core.browser.encodeURI
import com.github.ldaniels528.meansjs.util.ScalaJsHelper._
import com.github.ldaniels528.trifecta.sjs.models.{Message, Query, QueryResultSet, QueryRow}

import scala.concurrent.ExecutionContext
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{global => g}

/**
  * Query Service
  * @author lawrence.daniels@gmail.com
  */
class QueryService($http: Http) extends Service {

  def executeQuery(name: String, topic: String, queryString: String)(implicit ec: ExecutionContext) = {
    $http.post[QueryResultSet](
      url = "/api/query/all",
      data = js.Dictionary("name" -> name, "topic" -> topic, "queryString" -> queryString),
      headers = js.Dictionary("Content-Type" -> "application/json")) map (_.data)
  }

  def findOne(topic: String, criteria: String)(implicit ec: ExecutionContext) = {
    $http.get[Message](s"/api/query/one/$topic/${encodeURI(criteria)}") map (_.data)
  }

  def getQueries(implicit ec: ExecutionContext) = {
    $http.get[js.Array[Query]]("/api/queries") map (_.data)
  }

  def getQueriesByTopic(topic: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[Query]](s"/api/query/${encodeURI(topic)}") map (_.data)
  }

  def saveQuery(name: String, topic: String, queryString: String)(implicit ec: ExecutionContext) = {
    $http.post[Message](
      url = "/api/query",
      data = js.Dictionary("name" -> name, "topic" -> topic, "queryString" -> queryString),
      headers = js.Dictionary("Content-Type" -> "application/json")) map (_.data)
  }

  def transformResultsToCSV(queryResults: js.Array[QueryRow])(implicit ec: ExecutionContext) = {
    $http.post[js.Array[String]](
      url = "/api/results/csv",
      data = queryResults,
      headers = js.Dictionary("Content-Type" -> "application/json"),
      responseType = "arraybuffer"
    ) map { data =>
      val blob = js.Dynamic.newInstance(g.Blob)(js.Array(data), js.Dictionary("type" -> "text/csv"))
      val objectUrl = g.URL.createObjectURL(blob)
      g.window.open(objectUrl)
    }
  }

}
