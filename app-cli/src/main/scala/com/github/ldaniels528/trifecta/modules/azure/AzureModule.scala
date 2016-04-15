package com.github.ldaniels528.trifecta.modules.azure

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.Date

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.trifecta.TxResultHandler.Ok
import com.github.ldaniels528.trifecta.command.{Command, UnixLikeArgs, UnixLikeParams}
import com.github.ldaniels528.trifecta.io.{MessageInputSource, MessageOutputSource}
import com.github.ldaniels528.trifecta.modules.Module
import com.github.ldaniels528.trifecta.modules.azure.AzureModule.{AzureBlobDirectory, AzureBlobItem}
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob._

import scala.collection.JavaConversions._
import scala.language.postfixOps

/**
  * Azure Module
  * @author lawrence.daniels@gmail.com
  */
class AzureModule(config: TxConfig) extends Module {
  private var storageAccount: Option[CloudStorageAccount] = None
  private var blobClient: Option[CloudBlobClient] = None
  private var blobContainer: Option[CloudBlobContainer] = None

  /**
    * Returns the commands that are bound to the module
    * @return the commands that are bound to the module
    */
  override def getCommands(implicit rt: TxRuntimeContext) = Seq(
    Command(this, "blobconnect", connect, UnixLikeParams(Seq("connectionString" -> false)), help = "Establishes a connection to a Blob storage account"),
    Command(this, "blobcontainer", selectContainer, UnixLikeParams(Seq("container" -> true)), help = "Selects a Blob storage container"),
    Command(this, "blobls", listBlobs, UnixLikeParams(Nil, Nil), help = "Retrieves a list of files in the current container"),
    Command(this, "blobdl", downloadBlob, UnixLikeParams(Seq("prefix" -> true, "targetPath" -> true)), help = "Downloads a file to the current container"),
    Command(this, "blobdlr", downloadBlobs, UnixLikeParams(Seq("prefix" -> true), Nil), help = "Downloads all files to the current container"),
    Command(this, "blobup", uploadBlob, UnixLikeParams(Seq("sourcePath" -> true, "targetName" -> true)), help = "Uploads a file to the current container")
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
  override def getOutputSource(url: String): Option[MessageOutputSource] = None

  /**
    * Attempts to retrieve an input source for the given URL
    * @param url the given input URL
    * @return the option of an input source
    */
  override def getInputSource(url: String): Option[MessageInputSource] = None

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
    * Returns the name of the prefix (e.g. Seq("file"))
    * @return the name of the prefix
    */
  override def supportedPrefixes: Seq[String] = Nil

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

  /**
    * Download a file to the current container
    * @example blobdownload /path/to/file targetname
    */
  def downloadBlob(params: UnixLikeArgs) = {
    blobContainer map { container =>
      // determine the source and target file paths
      val (prefix, targetFileName) = params.args match {
        case aSrcFilePath :: aTargetFileName :: Nil => (aSrcFilePath, aTargetFileName)
        case _ => dieSyntax(params)
      }

      blobContainer.map { container =>
        container.listBlobs(prefix) foreach { item =>
          download(item, new File(targetFileName))
        }
        Ok
      }
    }
  }

  /**
    * Downloads all files to the current container
    * @example blobdownloadall /download/path
    */
  def downloadBlobs(params: UnixLikeArgs) = {
    // determine the target directory path
    val targetDirectoryPath = params.args match {
      case aPath :: Nil => aPath
      case _ => dieSyntax(params)
    }

    // recursively download all files
    blobContainer.map { container =>
      download(container, new File(targetDirectoryPath))
      Ok
    }
  }

  private def download(container: CloudBlobContainer, targetDirectory: File): Unit = {
    container.listBlobs() foreach { item =>
      download(item, targetDirectory)
    }
  }

  private def download(item: ListBlobItem, targetDirectory: File): Unit = {
    item match {
      case blob: CloudBlob =>
        new FileOutputStream(new File(targetDirectory, blob.getName)) use blob.download
      case directory: CloudBlobDirectory =>
        val subContainer = directory.getContainer
        download(subContainer, new File(targetDirectory, subContainer.getName))
      case blob =>
        throw new IllegalArgumentException(s"Unexpected blob type - ${blob.getClass.getName}")
    }
  }

  /**
    * Lists all the blobs in the current container
    * @example bloblist
    */
  def listBlobs(params: UnixLikeArgs) = {
    blobContainer flatMap { container =>
      container.listBlobs().toList map {
        case blob: CloudBlob =>
          val props = blob.getProperties
          AzureBlobItem(
            name = blob.getName,
            contentType = props.getContentType,
            lastModified = props.getLastModified,
            length = props.getLength
          )
        case directory: CloudBlobDirectory =>
          val subContainer = directory.getContainer
          AzureBlobDirectory(subContainer.getName)
        case blob =>
          throw new IllegalArgumentException(s"Unexpected blob type - ${blob.getClass.getName}")
      }
    }
  }

  /**
    * Selects a Blob storage container
    * @example blobcontainer test
    */
  def selectContainer(params: UnixLikeArgs) = {
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
    * Uploads a file to the current container
    * @example blobupload /path/to/file targetname
    */
  def uploadBlob(params: UnixLikeArgs) = {
    blobContainer map { container =>
      // determine the source and target file paths
      val (srcFilePath, targetFileName) = params.args match {
        case aSrcFilePath :: aTargetFileName :: Nil => (aSrcFilePath, aTargetFileName)
        case _ => dieSyntax(params)
      }

      val srcFile = new File(srcFilePath)
      val blob = container.getBlockBlobReference(targetFileName)
      blob.upload(new FileInputStream(srcFile), srcFile.length())
      Ok
    }
  }

}

/**
  * Azure Module Companion
  * @author lawrence.daniels@gmail.com
  */
object AzureModule {

  case class AzureBlobItem(name: String, contentType: String, lastModified: Date, length: Long)

  case class AzureBlobDirectory(name: String)

}