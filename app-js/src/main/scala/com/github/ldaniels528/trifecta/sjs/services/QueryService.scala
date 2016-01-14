package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.scalascript.Service
import com.github.ldaniels528.scalascript.core.Http
import com.github.ldaniels528.trifecta.sjs.models.{Message, Query, QueryResultSet, QueryRow}

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{global => g}

/**
  * Query Service
  * @author lawrence.daniels@gmail.com
  */
class QueryService($http: Http) extends Service {

  def executeQuery(name: String, topic: String, queryString: String) = {
    $http.post[QueryResultSet](
      url = "/api/query/all",
      data = js.Dictionary("name" -> name, "topic" -> topic, "queryString" -> queryString),
      headers = js.Dictionary("Content-Type" -> "application/json"))
  }

  def findOne(topic: String, criteria: String) = {
    $http.get[Message](s"/api/query/one/$topic/${g.encodeURI(criteria)}")
  }

  def getQueries = $http.get[js.Array[Query]]("/api/queries")

  def getQueriesByTopic(topic: String) = {
    $http.get[js.Array[Query]](s"/api/query/${g.encodeURI(topic)}")
  }

  def saveQuery(name: String, topic: String, queryString: String) = {
    $http.post[Message](
      url = "/api/query",
      data = js.Dictionary("name" -> name, "topic" -> topic, "queryString" -> queryString),
      headers = js.Dictionary("Content-Type" -> "application/json"))
  }

  def transformResultsToCSV(queryResults: js.Array[QueryRow]) = {
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
