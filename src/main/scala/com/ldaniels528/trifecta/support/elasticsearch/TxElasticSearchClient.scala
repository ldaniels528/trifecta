package com.ldaniels528.trifecta.support.elasticsearch

import com.ning.http.client.Response
import dispatch.{Http, url}

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Client
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxElasticSearchClient(host: String, port: Int) {
  private val http = s"http://$host:$port"

  /**
   * Counts all documents within a given index of a give type
   * @return {"count":1,"_shards":{"total":5,"successful":5,"failed":0}}
   * @example GET /quotes/quote/_count?pretty
   */
  def count(index: String, indexType: String)(implicit ec: ExecutionContext): Future[String] = {
    GET(s"$index/$indexType/_count?pretty")
  }

  /**
   * Counts matching documents within a given index of a give type based on the given query
   * @return {"count":1,"_shards":{"total":5,"successful":5,"failed":0}}
   * @example GET /quotes/quote/_count?pretty <- { "query" : { "term" : { "symbol" : "AAPL" } } }
   */
  def count(index: String, indexType: String, query: String)(implicit ec: ExecutionContext): Future[String] = {
    GET(s"$index/$indexType/_count?pretty", query)
  }

  /**
   * Creates a new document
   * @return the JSON results
   * @example PUT /website/blog/123/_create
   */
  def create(index: String, indexType: String, id: String, doc: String)(implicit ec: ExecutionContext): Future[String] = {
    PUT(s"$index/$indexType/$id/_create", doc)
  }

  /**
   * Creates a new index
   * @param index the given index
   * @return the JSON results
   * @example PUT /quotes
   */
  def createIndex(index: String)(implicit ec: ExecutionContext): Future[String] = {
    PUT(s"$index")
  }

  /**
   * Creates a new index
   * @param index the given index
   * @param shards the number of shards for the index
   * @param replicas the number of replicas for the index
   * @return the JSON results
   * @example PUT /quotes <- { "settings" : { "number_of_shards" : 5, "number_of_replicas" : 5 } }
   */
  def createIndex(index: String, shards: Int, replicas: Int)(implicit ec: ExecutionContext): Future[String] = {
    PUT(s"$index", s"""{ "settings" : { "number_of_shards" : $shards, "number_of_replicas" : $replicas } }""")
  }

  /**
   * Deletes a document by ID
   * @param index the given index
   * @param indexType the given index type
   * @param id the given document ID
   * @return {"found":true,"_index":"foo2","_type":"foo2","_id":"foo2","_version":2}
   * @example DELETE /quotes/quote/MSFT
   */
  def delete(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[String] = {
    DELETE(s"$index/$indexType/$id")
  }

  /**
   * Deletes the given index
   * @param index the given index
   * @return the JSON result
   * @example DELETE /twitter/
   */
  def deleteIndex(index: String)(implicit ec: ExecutionContext): Future[String] = {
    DELETE(index)
  }

  /**
   * Returns the connected end-point
   * @return the end-point (e.g. "dev501:9200")
   */
  def endPoint: String = s"$host:$port"

  /**
   * Indicates whether the given document exists
   * @param index the given index
   * @param indexType the given index type
   * @param id the given index type
   * @return the JSON result
   */
  def existsDocument(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    HEAD(s"$index/$indexType/$id") map (_.getStatusCode == 200)
  }

  /**
   * Indicates whether the given index exists
   * @param index the given index
   * @return the JSON result
   */
  def existsIndex(index: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    HEAD(index) map (_.getStatusCode == 200)
  }

  /**
   * Indicates whether the given type exists
   * @param index the given index
   * @param indexType the given index type
   * @return the JSON result
   */
  def existsType(index: String, indexType: String)(implicit ec: ExecutionContext): Future[Boolean] = {
    HEAD(s"$index/$indexType") map (_.getStatusCode == 200)
  }

  /**
   * Retrieves a document by ID
   * @param index the given index
   * @param indexType the given index type
   * @param id the given document ID
   * @return {"_index":"foo2","_type":"foo2","_id":"foo2","_version":1,"found":true,"_source":{"foo2":"bar"}}
   * @example GET /twitter/tweet/1
   */
  def get(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[String] = {
    GET(s"$index/$indexType/$id")
  }

  /**
   * Retrieves the settings for a given index
   * @param index the given index
   * @return {"quotes":{"settings":{"index":{"uuid":"_6Xom8GoTJymn6qvA2UO_w","number_of_replicas":"1","number_of_shards":"1","version":{"created":"1030299"}}}}}
   */
  def getSettings(index: String)(implicit ec: ExecutionContext): Future[String] = {
    GET(s"$index/_settings")
  }

  /**
   * Returns the cluster's health information
   * @return the cluster's health information
   * @example GET /_cluster/health?pretty
   */
  def health(implicit ec: ExecutionContext): Future[String] = GET("_cluster/health?pretty")

  /**
   * Matches all results for the given index
   * @param index the given index (e.g. "quotes")
   * @example GET /quotes/_search?pretty
   */
  def matchAll(index: String)(implicit ec: ExecutionContext): Future[String] = GET(s"$index/_search?pretty")

  /**
   * Queries all nodes
   * @return the nodes as JSON
   * @example GET /_nodes?pretty
   */
  def nodes(implicit ec: ExecutionContext): Future[String] = GET("_nodes?pretty")

  /**
   * Performs a search
   * @return the search results as JSON
   * @example GET /quotes/quote/_search?q=symbol:AAPL&pretty
   */
  def search(index: String, indexType: String, searchTerm: (String, String))(implicit ec: ExecutionContext): Future[String] = {
    GET(s"$index/$indexType/_search?q=${searchTerm._1}:${searchTerm._2}&pretty")
  }

  /**
   * Returns the server information
   * @return the server information as JSON
   * @example GET /
   */
  def serverInfo(implicit ec: ExecutionContext): Future[String] = GET("/")

  /**
   * Updates the settings for the given index
   * @example PUT /my_index/_settings <- { "index" : { "number_of_replicas" : 4 } }
   */
  def updateIndexSettings(index: String, replicas: Int)(implicit ec: ExecutionContext): Future[String] = {
    PUT(s"$index/_settings", """{ "index" : { "number_of_replicas" : $replicas } }""")
  }

  private def DELETE(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").DELETE OK dispatch.as.String)
  }

  private def GET(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command") OK dispatch.as.String)
  }

  private def GET(command: String, params: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command") << params OK dispatch.as.String)
  }

  private def HEAD(command: String)(implicit ec: ExecutionContext): Future[Response] = {
    Http(url(s"$http/$command").HEAD)
  }

  private def PUT(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").PUT OK dispatch.as.String)
  }

  private def PUT(command: String, params: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").PUT << params OK dispatch.as.String)
  }

  private def toMap(response: Response): Map[String, Seq[String]] = {
    Map((response.getHeaders.iterator() map (e => (e.getKey, e.getValue.toSeq))).toSeq: _*)
  }

}
