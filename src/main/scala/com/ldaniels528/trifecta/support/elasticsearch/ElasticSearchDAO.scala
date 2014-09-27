package com.ldaniels528.trifecta.support.elasticsearch

import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO._
import com.ning.http.client.Response
import net.liftweb.json._
import org.slf4j.LoggerFactory
import wabisabi.Client

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Data Access Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchDAO(val client: Client) {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  implicit val formats = DefaultFormats

  def count(indices: Seq[String], types: Seq[String], query: String)(implicit ec: ExecutionContext): Future[CountResponse] = {
    client.count(indices, types, query) map convert[CountResponse]
  }

  def createDocument(index: String, `type`: String, id: Option[String], data: String, refresh: Boolean)(implicit ec: ExecutionContext): Future[AddDocumentResponse] = {
    client.index(index, `type`, id, data, refresh) map convert[AddDocumentResponse]
  }

  def createIndex(name: String, settings: Option[String] = None)(implicit ec: ExecutionContext): Future[Boolean] = {
    client.createIndex(name, settings) map convert[AcknowledgeResponse] map (_.acknowledged)
  }

  def delete(index: String, `type`: String, id: String)(implicit ec: ExecutionContext): Future[DeleteResponse] = {
    client.delete(index, `type`, id) map convert[DeleteResponse]
  }

  def get(index: String, `type`: String, id: String)(implicit ec: ExecutionContext): Future[JValue] = {
    // response: {"_index":"quotes","_type":"quote","_id":"MSFT","_version":1,"found":true,
    //            "_source":{ symbol : "MSFT", lastSale : 31.33 }}
    client.get(index, `type`, id) map (_.getResponseBody) map parse map (_ \ "_source")
  }

  def matchAll(index: String)(implicit ec: ExecutionContext): Future[Response] = {
    client.search(index, query = """"query": { "match_all": { } }""")
  }

  def health()(implicit ec: ExecutionContext): Future[ClusterStatusResponse] = {
    client.health() map convert[ClusterStatusResponse]
  }

  def search(index: String, query: String)(implicit ec: ExecutionContext): Future[JValue] = {
    client.search(index, query) map checkForError map (_.getResponseBody) map parse
  }

  private def convert[T](response: Response)(implicit m: Manifest[T]): T = {
    checkForError(response)
    parse(response.getResponseBody).extract[T]
  }

  /**
   * Checks the response for errors
   * @param response the given [[Response]]
   */
  private def checkForError(response: Response): Response = {
    if (response.getStatusCode < 200 || response.getStatusCode >= 300) {
      val errorStatus = parse(response.getResponseBody).extract[ErrorResponse]
      throw new IllegalStateException(errorStatus.error)
    }
    response
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
    new ElasticSearchDAO(new Client(s"http://$host:$port"))
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