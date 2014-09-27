package com.ldaniels528.trifecta.modules.elasticSearch

import java.io.PrintStream

import com.ldaniels528.trifecta.command.{Command, UnboundedParams, UnixLikeArgs, UnixLikeParams}
import com.ldaniels528.trifecta.modules.Module
import com.ldaniels528.trifecta.modules.Module.NameValuePair
import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO
import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO.{AddDocumentResponse, CountResponse}
import com.ldaniels528.trifecta.vscript.Variable
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import net.liftweb.json._

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
 * Elastic Search Module
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchModule(config: TxConfig) extends Module {
  private val out: PrintStream = config.out
  private var client_? : Option[ElasticSearchDAO] = None
  private var endPoint_? : Option[String] = None
  private var cursor_? : Option[ElasticCursor] = None

  /**
   * Returns the name of the module (e.g. "kafka")
   * @return the name of the module
   */
  override def moduleName: String = "elasticSearch"

  /**
   * Returns the commands that are bound to the module
   * @return the commands that are bound to the module
   */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq[Command](
    Command(this, "econnect", connect, UnixLikeParams(Seq("host" -> false, "port" -> false)), help = "Connects to an Elastic Search server"),
    Command(this, "ecount", count, UnboundedParams(3), help = "Counts documents based on a query"),
    Command(this, "ecursor", showCursor, UnixLikeParams(), help = "Displays the navigable cursor"),
    Command(this, "eget", getDocument, UnixLikeParams(Seq("index" -> false, "type" -> false, "id" -> false)), help = "Retrieves a document"),
    Command(this, "eindex", createIndex, UnixLikeParams(Seq("name" -> true, "settings" -> false)), help = "Retrieves a document"),
    Command(this, "eput", createDocument, UnixLikeParams(Seq("index" -> false, "type" -> false, "id" -> false)), help = "Creates or updates a document"),
    Command(this, "esearch", searchDocument, UnixLikeParams(Seq("index" -> false, "query" -> true)), help = "Searches for document via a user-defined query")
  )

  /**
   * Returns an Elastic Search output writer
   * es:/quotes/quote/AAPL
   */
  override def getOutput(path: String): Option[OutputWriter] = {
    client_? flatMap { client =>
      path.split("[/]").toList match {
        case index :: indexType :: id :: Nil =>
          Option(new ElasticSearchOutputWriter(client, index, indexType))
        case _ =>
          die(s"Invalid output path '$path'")
      }
    }
  }

  /**
   * Returns the variables that are bound to the module
   * @return the variables that are bound to the module
   */
  override def getVariables: Seq[Variable] = Nil

  override def prompt = {
    def cursor(c: ElasticCursor) = "%s/%s%s".format(c.index, c.indexType, c.id.map(id => s"/$id") getOrElse "")
    s"${endPoint_? getOrElse "$"}/${cursor_? map cursor getOrElse ""}"
  }

  /**
   * Called when the application is shutting down
   */
  override def shutdown(): Unit = ()

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
    client_? = Option(ElasticSearchDAO(host, port))
    client_?.map { client =>
      endPoint_? = Option(s"$host:$port")
      client.health() map { response =>
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
  }

  /**
   * Counts documents based on a query
   * @example ecount quotes quote { matchAll: { } }
   */
  def count(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[CountResponse]] = {
    val (index, docType, query) = params.args match {
      case anIndex :: aType :: aQuery :: Nil => (anIndex, aType, aQuery)
      case _ => dieSyntax(params)
    }
    client.count(indices = Seq(index), types = Seq(docType), query) map (Seq(_))
  }

  /**
   * Creates a new (or updates an existing) document within an index
   * @example eput quotes quote AAPL { "symbol" : "AAPL", "lastSale" : 105.11 }
   * @example eput quotes quote MSFT { "symbol" : "MSFT", "lastSale" : 31.33 }
   * @example eput quotes quote AMD { "symbol" : "AMD", "lastSale" : 3.33 }
   */
  def createDocument(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[Seq[AddDocumentResponse]] = {
    val (index, docType, id, data) = params.args match {
      case anIndex :: aType :: anId :: someData :: Nil => (anIndex, aType, Option(anId), someData)
      case anIndex :: aType :: someData :: Nil => (anIndex, aType, None, someData)
      case anId :: someData :: Nil => cursor_? map (c => (c.index, c.indexType, Option(anId), someData)) getOrElse dieCursor()
      case someData :: Nil => cursor_? map (c => (c.index, c.indexType, None, someData)) getOrElse dieCursor()
      case _ => dieSyntax(params)
    }

    setCursor(index, docType, id, client.createDocument(index, docType, id, data, refresh = true)) map (Seq(_))
  }

  /**
   * Creates a new index
   * @example eindex "foo2"
   */
  def createIndex(params: UnixLikeArgs)(implicit ec: ExecutionContext) = {
    val (index, settings) = params.args match {
      case aName :: Nil => (aName, None)
      case aName :: aSetting :: Nil => (aName, Option(aSetting))
      case _ => dieSyntax(params)
    }

    client.createIndex(index, settings) map (Seq(_))
  }

  /**
   * Retrieves an existing document within an index
   * @example eget quotes quote AAPL
   */
  def getDocument(params: UnixLikeArgs)(implicit ec: ExecutionContext): Future[String] = {
    val (index, docType, id) = params.args match {
      case anIndex :: aType :: aId :: Nil => (anIndex, aType, aId)
      case aId :: Nil => cursor_? map (c => (c.index, c.indexType, aId)) getOrElse dieCursor()
      case Nil => cursor_? map (c => (c.index, c.indexType, c.id getOrElse dieNoId())) getOrElse dieCursor()
      case _ => dieSyntax(params)
    }

    // retrieve the document
    setCursor(index, docType, Option(id), client.get(index, docType, id)) map (js => pretty(render(js)))
  }

  /**
   * Searches for document via a user-defined query
   * @example esearch { "match_all": { } }
   */
  def searchDocument(params: UnixLikeArgs): Future[String] = {
    val (index, query) = params.args match {
      case anIndex :: aQuery :: Nil => (anIndex, aQuery)
      case aQuery :: Nil => cursor_? map (c => (c.index, aQuery)) getOrElse dieCursor()
      case _ => dieSyntax(params)
    }

    client.search(index, query)
  }

  /**
   * Returns the navigable cursor
   * @example ecursor
   */
  def showCursor(params: UnixLikeArgs): Option[Seq[ElasticCursor]] = {
    cursor_? map (Seq(_))
  }

  private def client: ElasticSearchDAO = client_? getOrElse die("No Elastic Search connection. Use 'econnect'")

  private def dieCursor[S](): S = die[S]("No Elastic Search navigable cursor found")

  private def dieNoId[S](): S = die[S]("No Elastic Search document ID found")

  private def setCursor[T](index: String, docType: String, id: Option[String], response: Future[T]): Future[T] = {
    response.onComplete {
      case Success(_) => cursor_? = Option(ElasticCursor(index, docType, id))
      case Failure(_) =>
    }
    response
  }

  /**
   * Elastic Search Navigable Cursor
   */
  case class ElasticCursor(index: String, indexType: String, id: Option[String])

}
