package com.github.ldaniels528.trifecta.modules.azure

import java.io.File
import java.util.Date

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.TxResultHandler.Ok
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.modules.Module
import com.github.ldaniels528.trifecta.modules.azure.AzureModule.{AzureBlobContainer, AzureBlobItem}
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.{postfixOps, reflectiveCalls}

/**
  * Azure Module
  * @author lawrence.daniels@gmail.com
  */
class AzureModule(config: TxConfig) extends Module {
  private val logger = LoggerFactory.getLogger(getClass)
  private var storageAccount: Option[CloudStorageAccount] = None
  private var blobClient: Option[CloudBlobClient] = None
  private var blobContainer: Option[CloudBlobContainer] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "blobconnect", connect, UnixLikeParams(Seq("connectionString" -> false)), help = "Establishes a connection to a Blob storage account"),
    Command(this, "blobcontainer", selectBlobContainer, UnixLikeParams(Seq("container" -> true)), help = "Selects a Blob storage container"),
    Command(this, "blobcount", countBlobs, UnixLikeParams(Seq("path" -> false), flags = Seq("-r" -> "recursive")), help = "Retrieves a list of files in the current container"),
    Command(this, "blobls", listBlobs, UnixLikeParams(Seq("path" -> false), flags = Seq("-r" -> "recursive")), help = "Retrieves a list of files in the current container"),
    Command(this, "blobget", downloadBlobs, UnixLikeParams(Seq("prefix" -> false, "targetPath" -> true), flags = Seq("-r" -> "recursive")), help = "Downloads a file to the current container"),
    Command(this, "blobput", uploadBlobs, UnixLikeParams(Seq("sourcePath" -> true), flags = Seq("-r" -> "recursive")), help = "Recursively uploads files or directories to the current container")
  )

  /**
    * Called when the application is shutting down
    */
  override def shutdown() = {}

  /**
    * Attempts to retrieve an output source for the given URL
    * @param url the given output URL
    * @return the option of an output source
    */
  override def getOutputSource(url: String) = None

  /**
    * Attempts to retrieve an input source for the given URL
    * @param url the given input URL
    * @return the option of an input source
    */
  override def getInputSource(url: String) = None

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
  override def prompt = s"/${blobContainer.map(_.getName) getOrElse ""}"

  /**
    * Returns the name of the prefix (e.g. Seq("file"))
    * @return the name of the prefix
    */
  override def supportedPrefixes = Nil

  /**
    * Establishes a connection to a remote DocumentDB cluster
    * @example blobconnect shocktrade
    */
  def connect(params: UnixLikeArgs) = {
    val CONNECTION_KEY = "trifecta.azure.blobstorage.url"

    // determine the connection string
    val connectionString = params.args match {
      case Nil => config.getOrElse(CONNECTION_KEY, dieConfigKey(CONNECTION_KEY))
      case aContainerName :: Nil => aContainerName
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    storageAccount = CloudStorageAccount.parse(connectionString)
    blobClient = storageAccount.map(_.createCloudBlobClient())
    blobClient map (_ => Ok)
  }

  def countBlobs(params: UnixLikeArgs) = {
    // determine the prefix
    val prefix = params.args match {
      case Nil => None
      case aPath :: Nil => Some(aPath)
      case _ => dieSyntax(params)
    }

    // capture the recursive flag
    val recursive = params.flags.contains("-r")

    // count the files
    blobContainer flatMap (count(_, prefix, recursive))
  }

  /**
    * Downloads file(s) to the current container
    * @example blobget [-r] [prefix] target
    */
  def downloadBlobs(params: UnixLikeArgs) = {
    // determine the prefix and target path
    val (prefix, targetPath) = params.args match {
      case aTargetFileName :: Nil => (None, aTargetFileName)
      case aPrefix :: aTargetPath :: Nil => (Some(aPrefix), aTargetPath)
      case _ => dieSyntax(params)
    }

    // capture the recursive flag
    val recursive = params.flags.contains("-r")

    // download the files
    blobContainer map { container =>
      val count = download(container, prefix, new File(targetPath), recursive)
      logger.info(s"$count file(s) downloaded")
      Ok
    }
  }

  /**
    * Lists all the blobs in the current container
    * @example blobls
    */
  def listBlobs(params: UnixLikeArgs) = {
    // determine the prefix
    val prefix = params.args match {
      case Nil => None
      case aPath :: Nil => Some(aPath)
      case _ => dieSyntax(params)
    }

    // capture the recursive flag
    val recursive = params.flags.contains("-r")

    // return the list of files
    blobContainer flatMap (list(_, prefix, recursive))
  }

  /**
    * Selects a Blob storage container
    * @example blobcontainer test
    */
  def selectBlobContainer(params: UnixLikeArgs) = {
    // determine the container name
    val containerName = params.args match {
      case aContainerName :: Nil => aContainerName
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    blobContainer = blobClient.map(_.getContainerReference(containerName.toLowerCase))
    blobContainer map (_ => Ok)
  }

  /**
    * Recursively uploads files or directories to the current container
    * @example blobput /path/to/file targetname
    */
  def uploadBlobs(params: UnixLikeArgs) = {
    // determine the source file path
    val srcFilePath = params.args match {
      case aSrcFilePath :: Nil => aSrcFilePath
      case _ => dieSyntax(params)
    }

    // capture the recursive flag
    val recursive = params.flags.contains("-r")

    // upload the files
    blobContainer map { container =>
      val count = upload(container, new File(srcFilePath), parentFile = None, recursive)
      logger.info(s"$count file(s) uploaded")
      Ok
    }
  }

  private def count(container: AzureBlobContainer, prefix: Option[String], recursive: Boolean): Int = {
    (prefix.map(container.listBlobs) getOrElse container.listBlobs()).foldLeft[Int](0) {
      case (total, directory: CloudBlobDirectory) if recursive => total + count(directory, prefix, recursive)
      case (total, blob) => total + 1
    }
  }

  private def download(container: AzureBlobContainer, prefix: Option[String], targetDirectory: File, recursive: Boolean): Int = {
    (prefix.map(container.listBlobs) getOrElse container.listBlobs()) map {
      case blob: CloudBlob =>
        val path = new File(targetDirectory, blob.getName).getAbsolutePath
        logger.info(s"Downloading '$path' (${blob.getProperties.getLength} bytes)...")
        blob.downloadToFile(path)
        1
      case directory: CloudBlobDirectory =>
        if (!recursive) 0
        else download(directory, prefix, new File(targetDirectory, directory.getContainer.getName), recursive)
      case blob =>
        throw new IllegalArgumentException(s"Unexpected blob type - ${blob.getClass.getName}")
    } sum
  }

  private def list(container: AzureBlobContainer, prefix: Option[String], recursive: Boolean): List[AzureBlobItem] = {
    (prefix.map(container.listBlobs) getOrElse container.listBlobs()).toList flatMap {
      case blob: CloudBlob =>
        val props = blob.getProperties
        AzureBlobItem(
          name = blob.getName,
          blobType = props.getBlobType.name(),
          contentType = props.getContentType,
          lastModified = props.getLastModified,
          length = props.getLength) :: Nil
      case directory: CloudBlobDirectory =>
        if (recursive) list(directory, prefix, recursive)
        else AzureBlobItem(name = directory.getUri.getPath.split("[/]").last, blobType = "FOLDER") :: Nil
      case blob =>
        AzureBlobItem(name = blob.getUri.getPath.split("[/]").last, blobType = "UNKNOWN") :: Nil
    }
  }

  private def upload(container: CloudBlobContainer, file: File, parentFile: Option[File], recursive: Boolean): Int = {
    file match {
      case f if f.isDirectory && recursive => f.listFiles() map (upload(container, _, f, recursive)) sum
      case f if f.isDirectory => 0
      case f =>
        logger.info(s"Uploading '${f.getAbsolutePath}' (${f.length()} bytes)...")
        val path = s"${parentFile.map(_.getName + "/") getOrElse ""}${f.getName}"
        val blob = container.getBlockBlobReference(path)
        blob.uploadFromFile(f.getAbsolutePath)
        1
    }
  }

}

/**
  * Azure Module Companion
  * @author lawrence.daniels@gmail.com
  */
object AzureModule {

  /**
    * Acts as a base class for blob containers
    */
  type AzureBlobContainer = {
    def listBlobs(): java.lang.Iterable[ListBlobItem]
    def listBlobs(prefix: String): java.lang.Iterable[ListBlobItem]
  }

  case class AzureBlobItem(name: String,
                           blobType: String,
                           contentType: Option[String] = None,
                           lastModified: Option[Date] = None,
                           length: Option[Long] = None)

}