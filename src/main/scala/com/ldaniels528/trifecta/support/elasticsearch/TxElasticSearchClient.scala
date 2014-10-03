package com.ldaniels528.trifecta.support.elasticsearch

import dispatch.{Http, url}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Client
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class TxElasticSearchClient(host: String, port: Int) {
  private val http = s"http://$host:$port"

  /**
   * Counts all documents within a given index of a give type
   * @return the count response
   * @example GET /quotes/quote/_count?pretty
   */
  def count(index: String, indexType: String)(implicit ec: ExecutionContext): Future[String] = {
    httpGet(s"$index/$indexType/_count?pretty")
  }

  /**
   * Counts matching documents within a given index of a give type based on the given query
   * @return the count response
   * @example GET /quotes/quote/_count?pretty <- { "query" : { "term" : { "symbol" : "AAPL" } } }
   */
  def count(index: String, indexType: String, query: String)(implicit ec: ExecutionContext): Future[String] = {
    httpGet(s"$index/$indexType/_count?pretty", query)
  }

  /**
   * Creates a new document
   * @return
   * @example PUT /website/blog/123/_create
   */
  def create(index: String, indexType: String, id: String, doc: String)(implicit ec: ExecutionContext): Future[String] = {
    httpPut(s"$index/$indexType/$id/_create", doc)
  }

  /**
   * Creates a new index
   * @param index the given index
   * @param shards the number of shards for the index
   * @param replicas the number of replicas for the index
   * @return the JSON results
   * @example GET /index <- { "settings" : { "number_of_shards" : 5, "number_of_replicas" : 5 } }
   */
  def createIndex(index: String, shards: Int, replicas: Int)(implicit ec: ExecutionContext): Future[String] = {
    httpGet(s"$index/", s"""{ "settings" : { "number_of_shards" : $shards, "number_of_replicas" : $replicas } }""")
  }

  /**
   * Deletes a document by ID
   * @param index the given index
   * @param indexType the given index type
   * @param id the given document ID
   * @example DELETE /quotes/quote/MSFT
   */
  def delete(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[String] = {
    httpDelete(s"$index/$indexType/$id")
  }

  /**
   * Deletes the given index
   * @param index the given index
   * @return
   * @example DELETE /twitter/
   */
  def deleteIndex(index: String)(implicit ec: ExecutionContext): Future[String] = {
    httpDelete(s"$index/")
  }

  /**
   * Retrieves a document by ID
   * @param index the given index
   * @param indexType the given index type
   * @param id the given document ID
   * @return the document if found
   * @example GET /twitter/tweet/1
   */
  def get(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[String] = {
    httpGet(s"$index/$indexType/$id")
  }

  /**
   * Returns the cluster's health information
   * @return the cluster's health information
   * @example GET /_cluster/health?pretty
   */
  def health(implicit ec: ExecutionContext): Future[String] = httpGet("_cluster/health?pretty")

  /**
   * Matches all results for the given index
   * @param index the given index (e.g. "quotes")
   * @example GET /quotes/_search?pretty
   */
  def matchAll(index: String)(implicit ec: ExecutionContext): Future[String] = httpGet(s"$index/_search?pretty")

  /**
   * Queries all nodes
   * @return the nodes as JSON
   * @example GET /_nodes?pretty
   */
  def nodes(implicit ec: ExecutionContext): Future[String] = httpGet("_nodes?pretty")

  /**
   * Performs a search
   * @return the search results as JSON
   * @example GET /quotes/quote/_search?q=symbol:AAPL&pretty
   */
  def search(index: String, indexType: String, searchTerm: (String, String))(implicit ec: ExecutionContext): Future[String] = {
    httpGet(s"$index/$indexType/_search?q=${searchTerm._1}:${searchTerm._2}&pretty")
  }

  /**
   * Returns the server information
   * @return the server information as JSON
   * @example GET /
   */
  def serverInfo(implicit ec: ExecutionContext): Future[String] = httpGet("/")

  private def httpDelete(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").DELETE OK dispatch.as.String)
  }

  private def httpGet(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command") OK dispatch.as.String)
  }

  private def httpGet(command: String, params: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command") << params OK dispatch.as.String)
  }

  private def httpPut(command: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").PUT OK dispatch.as.String)
  }

  private def httpPut(command: String, params: String)(implicit ec: ExecutionContext): Future[String] = {
    Http(url(s"$http/$command").PUT << params OK dispatch.as.String)
  }

}
