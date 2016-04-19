package com.github.ldaniels528.trifecta.modules.azure

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.TxResultHandler.Ok
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.modules.Module
import com.github.ldaniels528.trifecta.modules.azure.AzureModule._
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.microsoft.azure.documentdb.ConsistencyLevel
import com.microsoft.azure.storage.CloudStorageAccount

import scala.language.{postfixOps, reflectiveCalls}
import scala.util.Try

/**
  * Azure Module
  * @author lawrence.daniels@gmail.com
  */
class AzureModule(config: TxConfig) extends Module {
  private var blobStorage: Option[AzureBlobStorage] = None
  private var documentDB: Option[AzureDocumentDB] = None
  private var storageAccount: Option[CloudStorageAccount] = None
  private var tableStorage: Option[AzureTableStorage] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    // storage account
    Command(this, "storageaccount", connect, UnixLikeParams(Seq("connectionString" -> false)), help = "Establishes a connection to a Blob storage account"),

    // blob storage
    Command(this, "blob", selectBlobContainer, UnixLikeParams(Seq("container" -> true)), help = "Selects an Azure Blob storage container"),
    Command(this, "blobcount", countBlobs, UnixLikeParams(Seq("path" -> false), flags = Seq("-r" -> "recursive")), help = "Retrieves a list of files in the current container"),
    Command(this, "blobls", listBlobs, UnixLikeParams(Seq("path" -> false), flags = Seq("-r" -> "recursive")), help = "Retrieves a list of files in the current container"),
    Command(this, "blobget", downloadBlobs, UnixLikeParams(Seq("prefix" -> false, "targetPath" -> true), flags = Seq("-r" -> "recursive")), help = "Downloads a file to the current container"),
    Command(this, "blobput", uploadBlobs, UnixLikeParams(Seq("sourcePath" -> true), flags = Seq("-r" -> "recursive")), help = "Recursively uploads files or directories to the current container"),

    // DocumentDB
    Command(this, "dbconnect", databaseConnect, UnixLikeParams(Seq("host" -> false, "masterKey" -> false, "database" -> true, "collection" -> true, "consistencyLevel" -> false)), help = "Establishes a connection to a DocumentDB instance"),
    Command(this, "dbfind", findDocuments, UnixLikeParams(Seq("query" -> true), Seq("-o" -> "outputSource")), help = "Retrieves a document from DocumentDB"),
    Command(this, "dbnext", getNextMessage, UnixLikeParams(Nil, flags = Seq("-a" -> "avroCodec", "-f" -> "format", "-o" -> "outputSource")), help = "Attempts to retrieve the next message"),
    Command(this, "dbput", insertDocument, UnixLikeParams(Seq("json" -> true)), help = "Inserts a document into DocumentDB"),

    // table storage
    Command(this, "table", selectTable, UnixLikeParams(Seq("table" -> true)), help = "Selects (or creates) an Azure table"),
    Command(this, "tablels", listTables, UnixLikeParams(Seq("table" -> false)), help = "Lists Azure tables"),
    Command(this, "tablerm", deleteTable, UnixLikeParams(Seq("table" -> false)), help = "Deletes an existing Azure table")
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
      conn <- documentDB.map(_.connection)
    } yield new DocumentDbMessageOutputSource(conn)
  }

  /**
    * Returns the label of the module (e.g. "kafka")
    * @return the label of the module
    */
  override def moduleLabel = "azure"

  /**
    * Returns the name of the module (e.g. "kafka")
    * @return the name of the module
    */
  override def moduleName = "azure"

  /**
    * Returns the the information that is to be displayed while the module is active
    * @return the the information that is to be displayed while the module is active
    */
  override def prompt = s"/${blobStorage.flatMap(_.getContainerName) getOrElse ""}"

  /**
    * Called when the application is shutting down
    */
  override def shutdown() {
    Try(blobStorage.foreach(_.close()))
    Try(documentDB.foreach(_.close()))
    Try(tableStorage.foreach(_.close()))
    ()
  }

  /**
    * Returns the name of the prefix (e.g. Seq("file"))
    * @return the name of the prefix
    */
  override def supportedPrefixes = Seq("documentdb")

  ///////////////////////////////////////////////////////////////////////
  //    Storage Account
  ///////////////////////////////////////////////////////////////////////

  /**
    * Establishes a connection to a remote DocumentDB cluster
    * @example storageaccount shocktrade
    */
  def connect(params: UnixLikeArgs) = {
    // determine the connection string
    val connectionString = params.args match {
      case Nil => config.getOrElse(STORAGE_ACCOUNT_URL, dieConfigKey(STORAGE_ACCOUNT_URL))
      case aContainerName :: Nil => aContainerName
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    storageAccount = CloudStorageAccount.parse(connectionString)
    blobStorage = storageAccount.map(new AzureBlobStorage(_))
    tableStorage = storageAccount.map(new AzureTableStorage(_))
    storageAccount map (_ => Ok)
  }

  ///////////////////////////////////////////////////////////////////////
  //    Blob Storage
  ///////////////////////////////////////////////////////////////////////

  /**
    * Counts the blobs in the current container
    * @example blobcount
    */
  def countBlobs(params: UnixLikeArgs) = blobStorage.flatMap(_.countBlobs(params))

  /**
    * Downloads file(s) to the current container
    * @example blobget [-r] [prefix] target
    */
  def downloadBlobs(params: UnixLikeArgs) = blobStorage.flatMap(_.downloadBlobs(params))

  /**
    * Lists the blobs in the current container
    * @example blobls
    */
  def listBlobs(params: UnixLikeArgs) = blobStorage.flatMap(_.listBlobs(params))

  /**
    * Selects an Azure Blob storage container
    * @example blob test
    */
  def selectBlobContainer(params: UnixLikeArgs) = blobStorage.flatMap(_.selectBlobContainer(params))

  /**
    * Recursively uploads files or directories to the current container
    * @example blobput /path/to/file targetname
    */
  def uploadBlobs(params: UnixLikeArgs) = blobStorage.flatMap(_.uploadBlobs(params))

  ///////////////////////////////////////////////////////////////////////
  //    DocumentDB
  ///////////////////////////////////////////////////////////////////////

  /**
    * Establishes a connection to a remote DocumentDB cluster
    * @example dbconnect broadway powerball_history
    */
  def databaseConnect(params: UnixLikeArgs) = {
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
    documentDB.foreach(_.close())
    documentDB = new AzureDocumentDB(this, config, TxDocumentDbConnection(host, masterKey, databaseName, collectionName, consistencyLevel))
  }

  /**
    * Retrieves a list of DocumentDB documents
    * @example dbfind "SELECT TOP 10 * FROM powerball_history"
    * @example dbfind "SELECT TOP 10 * FROM powerball_history" -o es:/quotes/quote/AAPL
    * @example dbfind "SELECT TOP 10 * FROM powerball_history" -o topic:com.shocktrade.stocks.avro -a file:avro/quotes.avsc
    */
  def findDocuments(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = documentDB.flatMap(_.findDocuments(params))

  /**
    * Optionally returns the next message
    * @example dnext
    */
  def getNextMessage(params: UnixLikeArgs)(implicit rt: TxRuntimeContext) = documentDB.flatMap(_.getNextMessage(params))

  /**
    * Inserts a document into the database
    * @example dput { "symbol" : "AAPL", "lastTrade":99.56, "exchange":"NYSE" }
    */
  def insertDocument(params: UnixLikeArgs) = documentDB.flatMap(_.insertDocument(params))

  ///////////////////////////////////////////////////////////////////////
  //    Table Storage
  ///////////////////////////////////////////////////////////////////////

  /**
    * Deletes an existing Azure table
    * @example tablerm [tableName]
    */
  def deleteTable(params: UnixLikeArgs) = tableStorage.flatMap(_.deleteTable(params))

  /**
    * Lists tables
    * @example tablels [tableName]
    */
  def listTables(params: UnixLikeArgs) = tableStorage.flatMap(_.listTables(params))

  /**
    * Selects an Azure table
    * @example table tableName
    */
  def selectTable(params: UnixLikeArgs) = tableStorage.flatMap(_.selectTable(params))

}

/**
  * Azure Module Companion
  * @author lawrence.daniels@gmail.com
  */
object AzureModule {
  val STORAGE_ACCOUNT_URL = "trifecta.azure.storage.account.url"

}