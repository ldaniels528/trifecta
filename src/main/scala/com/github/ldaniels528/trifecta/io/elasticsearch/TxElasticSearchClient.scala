package com.github.ldaniels528.trifecta.io.elasticsearch

import java.net.URLEncoder.encode
import java.util.concurrent.Executors

import com.ning.http.client.Response
import dispatch._

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Client
 * @author lawrence.daniels@gmail.com
 */
class TxElasticSearchClient(host: String, port: Int) {
  private val pool = Executors.newFixedThreadPool(8)
  private val http$ = createHttpClient()
  private val http = s"http://$host:$port"

  /**
   * Closes the given index
   * @param index the given index
   * @return the [[Response]]
   */
  def closeIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = POST(s"$index/_close")

  /**
   * Counts all documents within a given index of a give type
   * @param index the given index
   * @param indexType the given index type
   * @return {"count":1,"_shards":{"total":5,"successful":5,"failed":0}}
   * @example GET /quotes/_count?pretty
   * @example GET /quotes/_count?q=symbol:AAPL&pretty
   * @example GET /quotes/quote/_count?pretty <- { "query" : { "term" : { "symbol" : "AAPL" } } }
   */
  def count(index: String, indexType: String = "", query: String = "", term: Option[(String, String)] = None)(implicit ec: ExecutionContext): Future[Response] = {
    val myType = if (indexType.nonEmpty) indexType + "/" else ""
    val myTerm = term map { case (key, value) => s"?q=$key:${encode(value, "UTF8")}"} getOrElse ""
    val url = s"$index/${myType}_count$myTerm"
    GET(url, query)
  }

  /**
   * Creates a new document
   * @param index the given index
   * @param indexType the given index type
   * @return the JSON results
   * @example PUT /website/blog/123/_create
   */
  def create(index: String, indexType: String, id: String, doc: String)(implicit ec: ExecutionContext): Future[Response] = {
    PUT(s"$index/$indexType/$id/_create", doc)
  }

  /**
   * Creates a new index
   * @param index the given index
   * @return the JSON results
   * @example PUT /quotes
   */
  def createIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = PUT(index)

  /**
   * Creates a new index
   * @param index the given index
   * @param shards the number of shards for the index
   * @param replicas the number of replicas for the index
   * @return the JSON results
   * @example PUT /quotes <- { "settings" : { "number_of_shards" : 5, "number_of_replicas" : 5 } }
   */
  def createIndex(index: String, shards: Int, replicas: Int)(implicit ec: ExecutionContext): Future[Response] = {
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
  def delete(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[Response] = {
    DELETE(s"$index/$indexType/$id")
  }

  /**
   * Deletes the given index
   * @param index the given index
   * @return the JSON result
   * @example DELETE /twitter/
   */
  def deleteIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = DELETE(index)

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
   * @example GET /quotes/quote/AAPL
   */
  def get(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[Response] = {
    GET(s"$index/$indexType/$id")
  }

  /**
   * Retrieves the settings for a given index
   * @param index the given index
   * @return {"quotes":{"settings":{"index":{"uuid":"_6Xom8GoTJymn6qvA2UO_w","number_of_replicas":"1","number_of_shards":"1","version":{"created":"1030299"}}}}}
   */
  def getSettings(index: String)(implicit ec: ExecutionContext): Future[Response] = GET(s"$index/_settings")

  /**
   * Returns the cluster's health information
   * @return the cluster's health information
   * @example GET /_cluster/health?pretty
   */
  def health(implicit ec: ExecutionContext): Future[Response] = GET("_cluster/health?pretty")

  /**
   * Matches all results for the given index
   * @param index the given index (e.g. "quotes")
   * @example GET /quotes/_search?pretty
   */
  def matchAll(index: String)(implicit ec: ExecutionContext): Future[Response] = GET(s"$index/_search?pretty")

  /**
   * Queries all nodes
   * @return the nodes as JSON
   * @example GET /_nodes?pretty
   */
  def nodes(implicit ec: ExecutionContext): Future[Response] = GET("_nodes")

  /**
   * Opens an index
   * @param index the given index
   * @return the [[Response]]
   */
  def openIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = POST(s"$index/_open")

  /**
   * Optimizes an index
   * @param index the given index
   * @return the [[Response]]
   */
  def optimizeIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = POST(s"$index/_optimize")

  /**
   * Performs a search
   * @return the search results as JSON
   * @example GET /quotes/quote/_search?q=symbol:AAPL&pretty
   */
  def search(index: String, indexType: String, searchTerm: (String, String))(implicit ec: ExecutionContext): Future[Response] = {
    GET(s"$index/$indexType/_search?q=${searchTerm._1}:${searchTerm._2}&pretty")
  }

  /**
   * Returns the server information
   * @return the server information as JSON
   * @example GET /
   */
  def serverInfo(implicit ec: ExecutionContext): Future[Response] = GET("/")

  /**
   * Returns the server status
   * @return the [[Response]]
   * @example GET /_status
   */
  def serverStatus(implicit ec: ExecutionContext): Future[Response] = GET("/_status")

  /**
   * Returns the status of the given index
   * @param index the given index
   * @return the [[Response]]
   */
  def statusIndex(index: String)(implicit ec: ExecutionContext): Future[Response] = GET(s"$index/_status")

  /**
   * Updates the settings for the given index
   * @example PUT /my_index/_settings <- { "index" : { "number_of_replicas" : 4 } }
   */
  def updateIndexSettings(index: String, replicas: Int)(implicit ec: ExecutionContext): Future[Response] = {
    PUT(s"$index/_settings", s"""{ "index" : { "number_of_replicas" : $replicas } }""")
  }

  /**
   * Performs an HTTP DELETE operation
   * @param command the given command or query
   * @return the [[Response]]
   */
  private def DELETE(command: String)(implicit ec: ExecutionContext): Future[Response] = {
    http$(url(s"$http/$command").DELETE)
  }

  /**
   * Performs an HTTP GET operation
   * @param command the given command or query
   * @return the [[Response]]
   */
  private def GET(command: String, params: String = "", pretty: Boolean = true)(implicit ec: ExecutionContext): Future[Response] = {
    if (params.isEmpty)
      http$(url(usePretty(s"$http/$command", pretty)))
    else
      http$(url(usePretty(s"$http/$command", pretty)) << params)
  }

  /**
   * Performs an HTTP HEAD operation
   * @param command the given command or query
   * @return the [[Response]]
   */
  private def HEAD(command: String)(implicit ec: ExecutionContext): Future[Response] = {
    http$(url(s"$http/$command").HEAD)
  }

  /**
   * Performs an HTTP POST operation
   * @param command the given command or query
   * @return the [[Response]]
   */
  private def POST(command: String, params: String = "")(implicit ec: ExecutionContext): Future[Response] = {
    if (params.isEmpty)
      http$(url(s"$http/$command").POST)
    else
      http$(url(s"$http/$command").POST << params)
  }

  /**
   * Performs an HTTP PUT operation
   * @param command the given command or query
   * @return the [[Response]]
   */
  private def PUT(command: String, params: String = "")(implicit ec: ExecutionContext): Future[Response] = {
    if (params.isEmpty)
      http$(url(s"$http/$command").PUT)
    else
      http$(url(s"$http/$command").PUT << params)
  }

  private def createHttpClient(): Http = {
    new Http() //with thread.Safety
      .configure(
        _.setAllowPoolingConnection(true)
          .setFollowRedirects(true)
          .setConnectionTimeoutInMs(1000)
          .setExecutorService(pool))
  }

  private def usePretty(query: String, pretty: Boolean): String = {
    if (!pretty) query
    else if (query.contains("?")) s"$query&pretty"
    else s"$query?pretty"
  }

}
