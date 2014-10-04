package com.ldaniels528.trifecta.modules.elasticSearch

import java.io.PrintStream

import com.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.modules.Module.NameValuePair
import com.ldaniels528.trifecta.modules.elasticSearch.ElasticSearchModule._
import com.ldaniels528.trifecta.support.elasticsearch.TxElasticSearchClient
import com.ldaniels528.trifecta.support.io.{InputSource, KeyAndMessage}
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.ning.http.client.Response
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Elastic Search Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchModule(config: TxConfig) extends Module {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private implicit val formats = DefaultFormats
  private val out: PrintStream = config.out
  private var client_? : Option[TxElasticSearchClient] = None
  private var cursor_? : Option[ElasticCursor] = None

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq[Command](
    Command(this, "econnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Connects to an Elastic Search server"),
    Command(this, "ecount", count, UnixLikeParams(Seq("path" -> true, "query" -> true)), help = "Counts documents based on a query"),
    Command(this, "ecursor", showCursor, UnixLikeParams(), help = "Displays the navigable cursor"),
    Command(this, "edelete", deleteDocumentOrIndex, UnixLikeParams(Seq("index" -> true)), help = "Deletes a new index (DESTRUCTIVE)"),
    Command(this, "eget", getDocument, UnixLikeParams(Seq("path" -> false), Seq("-o" -> "outputTo")), help = "Retrieves a document"),
    Command(this, "ehealth", health, UnixLikeParams(), help = "Retrieves the cluster's health information"),
    Command(this, "eindex", createIndex, UnixLikeParams(Seq("index" -> true, "shards" -> false, "replicas" -> false)), help = "Creates a new index"),
    Command(this, "ematchall", matchAll, UnixLikeParams(Seq("index" -> true)), help = "Retrieves all documents for a given index"),
    Command(this, "enodes", showNodes, UnixLikeParams(), help = "Returns information about the nodes in the cluster"),
    Command(this, "eput", createDocument, UnixLikeParams(Seq("path" -> true, "data" -> true)), help = "Creates or updates a document"),
    Command(this, "esearch", searchDocument, UnixLikeParams(Seq("index" -> false, "type" -> false, "field" -> true, "==" -> true, "value" -> true)), help = "Searches for document via a user-defined query"),
    Command(this, "eserverinfo", serverInfo, UnixLikeParams(), help = "Retrieves server information"),
    Command(this, "eexists", existsDocumentIndexOrType, UnixLikeParams(Seq("path" -> true)), help = "Tests whether the index or document exists")
  )

  /**
   * Returns an Elastic Search document input source
   * @param url the given input URL (e.g. "es:/quotes/quote/AAPL")
   * @return the option of an Elastic Search document input source
   */
  override def getInputHandler(url: String): Option[InputSource] = None

  /**
   * Returns an Elastic Search document output source
   * @param url the given input URL (e.g. "es:/quotes/quote/AAPL")
   * @return the option of an Elastic Search document output source
   */
  override def getOutputHandler(url: String): Option[DocumentOutputSource] = {
    url.extractProperty("es:") map { path =>
      path.split("[/]").toList match {
        case "" :: index :: indexType :: Nil =>
          new DocumentOutputSource(client, index, indexType, id = None)
        case "" :: index :: indexType :: id :: Nil =>
          new DocumentOutputSource(client, index, indexType, Option(id))
        case _ =>
          dieInvalidOutputURL(url, "es:/quotes/quote/AAPL")
      }
    }
  }

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  override def getVariables: Seq[Variable] = Nil

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  override def moduleName: String = "elasticSearch"

  /**
   * Returns the label of the module (e.g. "kafka")
   * @return the label of the module
   */
  override def moduleLabel = "es"

  override def prompt: String = {
    def cursor(c: ElasticCursor) = "%s/%s%s".format(c.index, c.indexType, c.id.map(id => s"/$id") getOrElse "")
    s"${client_? map (_.endPoint) getOrElse "$"}/${cursor_? map cursor getOrElse ""}"
  }

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

  override def supportedPrefixes: Seq[String] = Seq("es")

  /**
   * Establishes a connection to a remote host
   * @example econnect dev501 9200
   */
  def connect(params: UnixLikeArgs)(implicit ec: ExecutionContext): Option[Future[Seq[NameValuePair]]] = {
    val (host, port) = params.args match {
      case aHost :: aPort :: Nil => (aHost, parseInt("port", aPort))
      case aHost :: Nil => (aHost, 9200)
      case Nil => ("localhost", 9200)
      case _ => dieSyntax(params)
    }

    // connect to the server, return the health statistics
    out.println(s"Connecting to Elastic Search at '$host:$port'...")
    client_? = Option(new TxElasticSearchClient(host, port))
    client_?.map(client => health(params))
  }

  /**
   * Counts documents based on a query
   * @example ecount /quotes/quote { matchAll: { } }
   */
  def count(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[CountResponse]] = {
    val (index, docType, query) = params.args match {
      case aPath :: aQuery :: Nil => aPath.split("[/]").toList match {
        case anIndex :: aType :: Nil => (anIndex, aType, aQuery)
        case _ => dieSyntax(params)
      }
      case _ => dieSyntax(params)
    }

    client.count(index, docType, query) map convert[CountResponse] map (Seq(_))
  }

  /**
   * Creates a new (or updates an existing) document within an index
   * @example eput /quotes/quote/AAPL { "symbol" : "AAPL", "lastSale" : 105.11 }
   * @example eput /quotes/quote/MSFT { "symbol" : "MSFT", "lastSale" : 31.33 }
   * @example eput /quotes/quote/AMD { "symbol" : "AMD", "lastSale" : 3.33 }
   */
  def createDocument(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[AddDocumentResponse]] = {
    val (index, docType, id, data) = params.args match {
      case aPath :: someData :: Nil => aPath.split("[/]").toList match {
        case "" :: anIndex :: aType :: anId :: Nil => (anIndex, aType, anId, someData)
        case anIndex :: aType :: anId :: Nil => (anIndex, aType, anId, someData)
        case anId :: Nil => cursor_? map (c => (c.index, c.indexType, anId, someData)) getOrElse dieCursor()
        case _ => dieSyntax(params)
      }
      case _ => dieSyntax(params)
    }

    setCursor(index, docType, Option(id), client.create(index, docType, id, data)) map convert[AddDocumentResponse] map (Seq(_))
  }

  /**
   * Creates a new index
   * @example eindex foo2 1 2
   */
  def createIndex(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[AddDocumentResponse]] = {
    val (index, shards, replicas) = params.args match {
      case aName :: Nil => (aName, 1, 1)
      case aName :: aShards :: Nil => (aName, parseInt("shards", aShards), parseInt("replicas", aShards))
      case aName :: aShards :: aReplicas :: Nil => (aName, parseInt("shards", aShards), parseInt("replicas", aReplicas))
      case _ => dieSyntax(params)
    }

    client.createIndex(index, shards, replicas) map convert[AddDocumentResponse] map (Seq(_))
  }

  /**
   * Deletes a document or index
   * @example edelete quotes              [Delete the index "quotes"]
   * @example edelete /quotes/quote/AAPL  [Delete the document "AAPL" from "/quotes/quote"]
   */
  def deleteDocumentOrIndex(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[_] = {
    params.args match {
      case path :: Nil => extractPathComponents(params, path) match {
        case (index, None, None) => client.deleteIndex(index) map convert[DeleteResponse] map (Seq(_))
        case (index, Some(indexType), Some(id)) => client.delete(index, indexType, id)
        case _ => dieSyntax(params)
      }
      case _ => dieSyntax(params)
    }
  }

  /**
   * Tests whether an index, type or document exists
   * @example eexists quotes
   * @example eexists /quotes/quote
   * @example eexists /quotes/quote/AAPL
   */
  def existsDocumentIndexOrType(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Boolean] = {
    params.args match {
      case path :: Nil => extractPathComponents(params, path) match {
        case (index, None, None) => client.existsIndex(index)
        case (index, Some(indexType), None) => client.existsType(index, indexType)
        case (index, Some(indexType), Some(id)) => client.existsDocument(index, indexType, id)
        case _ => dieSyntax(params)
      }
      case _ => dieSyntax(params)
    }
  }

  /**
   * Retrieves an existing document within an index
   * @example eget /quotes/quote/AAPL
   */
  def getDocument(params: UnixLikeArgs)(implicit rt: TxRuntimeContext, ec: ExecutionContext): Future[JValue] = {
    val ESPath(index, docType, id) = params.args match {
      case path :: Nil => parsePath(params, path)
      case _ => dieSyntax(params)
    }

    // retrieve the document
    setCursor(index, docType, Option(id), client.get(index, docType, id) map parse map (_ \ "_source") map { js =>
      // handle the optional output directive
      val encoding = config.encoding
      val outputSource = getOutputSource(params)
      outputSource.foreach(_.write(KeyAndMessage(id.getBytes(encoding), compact(render(js)).getBytes(encoding))))
      js
    })
  }

  /**
   * Retrieves the cluster's health information
   * @example ehealth
   */
  def health(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[NameValuePair]] = {
    client.health map convert[ClusterStatusResponse] map { response =>
      Seq(
        NameValuePair("Cluster Name", response.cluster_name),
        NameValuePair("Status", response.status),
        NameValuePair("Timed Out", response.timed_out),
        NameValuePair("Number of Nodes", response.number_of_nodes),
        NameValuePair("Number of Data Nodes", response.number_of_data_nodes),
        NameValuePair("Active Shards", response.active_shards),
        NameValuePair("Active Primary Shards", response.active_primary_shards),
        NameValuePair("Initializing Shards", response.initializing_shards),
        NameValuePair("Relocating Shards", response.relocating_shards),
        NameValuePair("Unassigned Shards", response.unassigned_shards))
    }
  }

  /**
   * Retrieves all documents for a given index
   * @example ematchall quotes
   */
  def matchAll(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[String] = {
    val index = params.args match {
      case anIndex :: Nil => anIndex
      case _ => dieSyntax(params)
    }

    client.matchAll(index)
  }

  /**
   * Searches for document via a user-defined query
   * @example esearch field == value
   * @example esearch myIndex myType field == value
   */
  def searchDocument(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[String] = {
    val (index, indexType, field, value) = params.args match {
      case anIndex :: aType :: aField :: "==" :: aValue :: Nil => (anIndex, aType, aField, aValue)
      case aType :: aField :: "==" :: aValue :: Nil => cursor_? map (c => (c.index, aType, aField, aValue)) getOrElse dieCursor()
      case aField :: "==" :: aValue :: Nil => cursor_? map (c => (c.index, c.indexType, aField, aValue)) getOrElse dieCursor()
      case _ => dieSyntax(params)
    }

    client.search(index, indexType, field -> value)
  }

  /**
   * Retrieve server information for the currently connected host
   * @example eServerInfo
   * @example eServerInfo dev501 9200
   */
  def serverInfo(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[String] = {
    val host_port = params.args match {
      case Nil => None
      case host :: Nil => Option((host, 9200))
      case host :: port :: Nil => Option((host, parseInt("port", port)))
      case _ => dieSyntax(params)
    }
    client.serverInfo
  }

  /**
   * Returns the navigable cursor
   * @example ecursor
   */
  def showCursor(params: UnixLikeArgs): Option[Seq[ElasticCursor]] = {
    cursor_? map (Seq(_))
  }

  def showNodes(params: UnixLikeArgs): Future[String] = {
    client.nodes
  }

  private def client: TxElasticSearchClient = client_? getOrElse die("No Elastic Search connection. Use 'econnect'")

  private def dieCursor[S](): S = die[S]("No Elastic Search navigable cursor found")

  private def dieNoId[S](): S = die[S]("No Elastic Search document ID found")

  /**
   * Attempts to extract up to 3 components from the given path (index, type and ID)
   * @param params the given [[UnixLikeArgs]](Unix Style arguments)
   * @param path the given path (e.g. "/quotes/quote/AAPL")
   * @return the path components
   */
  private def extractPathComponents(params: UnixLikeArgs, path: String) = {
    val myPath = if (path.startsWith("/")) path.substring(1) else path
    myPath.split("[/]").toList match {
      case index :: Nil => (index, None, None)
      case index :: indexType :: Nil => (index, Option(indexType), None)
      case index :: indexType :: id :: Nil => (index, Option(indexType), Option(id))
      case _ => dieSyntax(params)
    }
  }

  private def parsePath(params: UnixLikeArgs, path: String): ESPath = {
    val (index, docType, id) = path.split("[/]").toList match {
      case "" :: anIndex :: aType :: aId :: Nil => (anIndex, aType, aId)
      case anIndex :: aType :: aId :: Nil => (anIndex, aType, aId)
      case aId :: Nil => cursor_? map (c => (c.index, c.indexType, aId)) getOrElse dieCursor()
      case _ => dieSyntax(params)
    }
    ESPath(index, docType, id)
  }

  private def setCursor[T](index: String, docType: String, id: Option[String], response: Future[T]): Future[T] = {
    response.onComplete {
      case Success(_) => cursor_? = Option(ElasticCursor(index, docType, id))
      case Failure(_) =>
    }
    response
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
 * Elastic Search Module Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ElasticSearchModule {

  /**
   * Elastic Search Navigable Cursor
   */
  case class ElasticCursor(index: String, indexType: String, id: Option[String])

  case class ESPath(index: String, docType: String, id: String)

  /**
   * {"acknowledged":true}
   */
  case class AcknowledgeResponse(acknowledged: Boolean)

  /**
   * {"_index":"foo2","_type":"foo2","_id":"foo2","_version":1,"created":true}
   */
  case class AddDocumentResponse(created: Boolean, _index: String, _type: String, _id: String, _version: Int)

  /**
   * {"cluster_name":"elasticsearch","status":"green","timed_out":false,"number_of_nodes":1,"number_of_data_nodes":1,
   * "active_primary_shards":0,"active_shards":0,"relocating_shards":0,"initializing_shards":0,"unassigned_shards":0}
   */
  case class ClusterStatusResponse(cluster_name: String, status: String, timed_out: Boolean, number_of_nodes: Int,
                                   number_of_data_nodes: Int, active_primary_shards: Int, active_shards: Int, relocating_shards: Int,
                                   initializing_shards: Int, unassigned_shards: Int)

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

}
