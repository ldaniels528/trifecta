package com.ldaniels528.trifecta.support.elasticsearch

import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO._
import com.ning.http.client.Response
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Data Access Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchDAO(val client: TxElasticSearchClient) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val formats = DefaultFormats

  def count(index: String, indexType: String, query: String)(implicit ec: ExecutionContext): Future[CountResponse] = {
    client.count(index, indexType, query) map convert[CountResponse]
  }

  def create(index: String, indexType: String, id: String, doc: String)(implicit ec: ExecutionContext): Future[AddDocumentResponse] = {
    client.create(index, indexType, id, doc) map convert[AddDocumentResponse]
  }

  def createIndex(name: String, shards: Int = 1, replicas: Int = 1)(implicit ec: ExecutionContext): Future[Boolean] = {
    client.createIndex(name, shards, replicas) map convert[AcknowledgeResponse] map (_.acknowledged)
  }

  def deleteIndex(index: String)(implicit ec: ExecutionContext): Future[DeleteResponse] = {
    client.deleteIndex(index) map convert[DeleteResponse]
  }

  def delete(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[DeleteResponse] = {
    client.delete(index, indexType, id) map convert[DeleteResponse]
  }

  def get(index: String, indexType: String, id: String)(implicit ec: ExecutionContext): Future[JValue] = {
    // response: {"_index":"quotes","_type":"quote","_id":"MSFT","_version":1,"found":true,
    //            "_source":{ symbol : "MSFT", lastSale : 31.33 }}
    client.get(index, indexType, id) map parse map (_ \ "_source")
  }

  def health()(implicit ec: ExecutionContext): Future[ClusterStatusResponse] = {
    client.health map convert[ClusterStatusResponse]
  }

  def matchAll(index: String)(implicit ec: ExecutionContext): Future[String] = {
    client.matchAll(index)
  }

  def nodes(implicit ec: ExecutionContext) = client.nodes

  def search(index: String, indexType: String, term: (String, String))(implicit ec: ExecutionContext): Future[String] = {
    client.search(index, indexType, term)
  }

  private def convert[T](response: String)(implicit m: Manifest[T]): T = {
    parse(response).extract[T]
  }

  private def show(response: Response): Response = {
    logger.info(s"response = ${response.getResponseBody}")
    response
  }

}

/**
 * Elastic Search DAO Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ElasticSearchDAO {

  /**
   * Creates a new Elastic Search DAO instance
   * @param host the given host name
   * @param port the given port number
   * @return a new Elastic Search DAO instance
   */
  def apply(host: String, port: Int = 9200): ElasticSearchDAO = {
    new ElasticSearchDAO(new TxElasticSearchClient(host, port))
  }

  /**
   * {"acknowledged":true}
   */
  case class AcknowledgeResponse(acknowledged: Boolean)

  /**
   * {"_index":"foo2","_type":"foo2","_id":"foo2","_version":1,"created":true}
   */
  case class AddDocumentResponse(created: Boolean, _index: String, _type: String, _id: String, _version: Int)

  /**
   * {"count":1,"_shards":{"total":5,"successful":5,"failed":0}}
   */
  case class CountResponse(count: Int, _shards: Shards)

  /**
   * {"total":5,"successful":5,"failed":0}
   */
  case class Shards(total: Int, successful: Int, failed: Int)

  /**
   * {"found":true,"_index":"foo2","_type":"foo2","_id":"foo2","_version":2}
   */
  case class DeleteResponse(found: Boolean, _index: String, _type: String, _id: String, _version: Int)

  /**
   * {"error":"IndexAlreadyExistsException[ [foo] already exists]", "status" : 400}
   */
  case class ErrorResponse(error: String, status: Int)

  /**
   * {"_index":"foo2","_type":"foo2","_id":"foo2","_version":1,"found":true,"_source":{"foo2":"bar"}}
   */
  case class FetchResponse(found: Boolean, _source: String, _index: String, _type: String, _id: String, _version: Int)

  /**
   * {"cluster_name":"elasticsearch","status":"green","timed_out":false,"number_of_nodes":1,"number_of_data_nodes":1,
   * "active_primary_shards":0,"active_shards":0,"relocating_shards":0,"initializing_shards":0,"unassigned_shards":0}
   */
  case class ClusterStatusResponse(cluster_name: String, status: String, timed_out: Boolean, number_of_nodes: Int,
                                   number_of_data_nodes: Int, active_primary_shards: Int, active_shards: Int, relocating_shards: Int,
                                   initializing_shards: Int, unassigned_shards: Int)

}