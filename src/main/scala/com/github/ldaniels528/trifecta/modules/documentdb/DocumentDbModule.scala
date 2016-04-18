package com.github.ldaniels528.trifecta.modules.documentdb

import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.TxResultHandler.Ok
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.io.KeyAndMessage
import com.github.ldaniels528.trifecta.io.avro.AvroCodec
import com.github.ldaniels528.trifecta.io.json.JsonHelper._
import com.github.ldaniels528.trifecta.messages.BinaryMessaging
import com.github.ldaniels528.trifecta.modules.Module
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.microsoft.azure.documentdb.{ConsistencyLevel, Document}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.Try

/**
  * Azure DocumentDB Module
  * @author lawrence.daniels@gmail.com
  */
class DocumentDbModule(config: TxConfig) extends Module with BinaryMessaging {
  private var conn_? : Option[TxDocumentDbConnection] = None
  private var cursor_? : Option[java.util.Iterator[Document]] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "dconnect", connect, UnixLikeParams(Seq("host" -> false, "masterKey" -> false, "database" -> true, "collection" -> true, "consistencyLevel" -> false)), help = "Establishes a connection to a DocumentDB instance"),
    Command(this, "dfind", findDocuments, UnixLikeParams(Seq("query" -> true), Seq("-o" -> "outputSource")), help = "Retrieves a document from DocumentDB"),
    Command(this, "dnext", getNextMessage, UnixLikeParams(Nil, flags = Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource")), help = "Attempts to retrieve the next message"),
    Command(this, "dput", insertDocument, UnixLikeParams(Seq("json" -> true)), help = "Inserts a document into DocumentDB")
  )

  /**
    * Attempts to retrieve an input source for the given URL
    * @param url the given input URL
    * @return the option of an input source
    */
  override def getInputSource(url: String) = None

  /**
    * Attempts to retrieve an output source for the given URL
    * @param url the given output URL
    * @return the option of an output source
    */
  override def getOutputSource(url: String) = {
    for {
      databaseName <- url.extractProperty("documentdb:")
      conn <- conn_?
    } yield new DocumentDbMessageOutputSource(conn)
  }

  /**
    * Returns the name of the prefix (e.g. Seq("file"))
    * @return the name of the prefix
    */
  override def supportedPrefixes = Seq("documentdb")

  /**
    * Returns the label of the module (e.g. "kafka")
    * @return the label of the module
    */
  override def moduleLabel = "documentdb"

  /**
    * Returns the name of the module (e.g. "kafka")
    * @return the name of the module
    */
  override def moduleName = "documentdb"

  /**
    * Returns the the information that is to be displayed while the module is active
    * @return the the information that is to be displayed while the module is active
    */
  override def prompt = conn_?.map(c => s"${c.databaseName}.${c.collectionName}").getOrElse("[default]")

  /**
    * Called when the application is shutting down
    */
  override def shutdown() = {}

  /**
    * Establishes a connection to a remote DocumentDB cluster
    * @example dconnect broadway powerball_history
    */
  def connect(params: UnixLikeArgs): Unit = {
    // determine the requested end-point
    val (host, masterKey, databaseName, collectionName, consistencyLevel) = params.args match {
      case aDatabase :: aCollection :: Nil =>
        val host = config.getOrElse("trifecta.documentdb.host", dieConfigKey("trifecta.documentdb.host"))
        val masterKey = config.getOrElse("trifecta.documentdb.masterKey", dieConfigKey("trifecta.documentdb.masterKey"))
        (host, masterKey, aDatabase, aCollection, ConsistencyLevel.Strong)
      case aHost :: aMasterKey :: aDatabase :: aCollection :: Nil =>
        (aHost, aMasterKey, aDatabase, aCollection, ConsistencyLevel.Strong)
      case aHost :: aMasterKey :: aDatabase :: aCollection :: aConsistencyLevel :: Nil =>
        (aHost, aMasterKey, aDatabase, aCollection, ConsistencyLevel.valueOf(aConsistencyLevel))
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    Try(conn_?.foreach(_.close()))
    conn_? = Option(TxDocumentDbConnection(host, masterKey, databaseName, collectionName, consistencyLevel))
  }

  /**
    * Retrieves a list of DocumentDB documents
    * @example dfind "SELECT TOP 10 * FROM powerball_history"
    * @example dfind "SELECT TOP 10 * FROM powerball_history" -o es:/quotes/quote/AAPL
    * @example dfind "SELECT TOP 10 * FROM powerball_history" -o topic:com.shocktrade.stocks.avro -a file:avro/quotes.avsc
    */
  def findDocuments(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    // retrieve the documents from the collection
    val response = params.args match {
      case List(query) => connection.queryDocuments(query)
      case _ => dieSyntax(params)
    }

    // extract the documents
    cursor_? = Option(response.getQueryIterable.iterator())

    // determine which decoder to use; either the user specified decoder, cursor's decoder or none
    val decoder = params("-a") map AvroCodec.resolve

    // write the document to an output source?
    val outputSource_? = getOutputSource(params)
    for {
      out <- outputSource_?
      _ = out.open()
      doc <- response.getQueryIterable.iterator()
    } {
      val key = doc.getId.getBytes(config.encoding)
      val value = doc.toString.getBytes(config.encoding)
      out.write(KeyAndMessage(key, value), decoder)
    }
    outputSource_?.foreach(_.close())

    nextMessage
  }

  /**
    * Optionally returns the next message
    * @example dnext
    */
  def getNextMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = {
    params.args match {
      case Nil => nextMessage
      case _ => dieSyntax(params)
    }
  }

  /**
    * Inserts a document into the database
    * @example dput { "symbol" : "AAPL", "lastTrade":99.56, "exchange":"NYSE" }
    */
  def insertDocument(params: UnixLikeArgs) = {
    val jsonString = params.args match {
      case List(aJsonString) => compressJson(aJsonString)
      case _ => dieSyntax(params)
    }

    // insert the document into the collection
    connection.createDocument(jsonString)
    Ok()
  }

  private def connection: TxDocumentDbConnection = conn_? getOrElse die("No DocumentDB connection defined")

  private def nextMessage = cursor_? flatMap { it =>
    if (it.hasNext) Option(toJson(it.next().toString)) else None
  }

}
