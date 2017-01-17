package com.github.ldaniels528.trifecta.modules.azure

import java.io.File
import java.util.Date

import com.github.ldaniels528.commons.helpers.OptionHelper.Risky._
import com.github.ldaniels528.trifecta.command.UnixLikeArgs
import com.github.ldaniels528.trifecta.modules.ModuleCommandAgent
import com.github.ldaniels528.trifecta.modules.azure.AzureBlobStorage.{AzureBlobContainer, AzureBlobItem, AzureContainer}
import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlob, CloudBlobContainer, CloudBlobDirectory, ListBlobItem}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.language.{postfixOps, reflectiveCalls}

/**
  * Azure Blob Storage
  * @author lawrence.daniels@gmail.com
  */
class AzureBlobStorage(storageAccount: CloudStorageAccount) extends ModuleCommandAgent {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val blobClient = storageAccount.createCloudBlobClient()
  private var blobContainer: Option[CloudBlobContainer] = None

  /**
    * Closes the connection
    */
  def close(): Unit = ()

  /**
    * Returns the name of the currently selected container
    * @return the name of the currently selected container
    */
  def getContainerName = blobContainer.map(_.getName)

  /**
    * Counts the blobs in the current container
    * @param params the given [[UnixLikeArgs arguments]]
    */
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
    * @param params the given [[UnixLikeArgs arguments]]
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
      ()
    }
  }

  /**
    * Lists all the blobs in the current container
    * @param params the given [[UnixLikeArgs arguments]]
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
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def selectBlobContainer(params: UnixLikeArgs) = {
    // determine the container name
    val containerName = params.args match {
      case aContainerName :: Nil => aContainerName
      case _ => dieSyntax(params)
    }

    // connect to the remote peer
    blobContainer = blobClient.getContainerReference(containerName)
    blobContainer foreach (_ => ())
  }

  /**
    * Recursively uploads files or directories to the current container
    * @param params the given [[UnixLikeArgs arguments]]
    */
  def uploadBlobs(params: UnixLikeArgs): Option[Any] = {
    // determine the source file path
    val srcFilePath = params.args match {
      case aSrcFilePath :: Nil => aSrcFilePath
      case _ => dieSyntax(params)
    }

    // capture the recursive flag
    val recursive = params.flags.contains("-r")
    if(recursive) logger.info("Recursive copy enabled")

    // upload the files
    blobContainer map { container =>
      val count = upload(container, new File(srcFilePath), parentFile = None, recursive)
      logger.info(s"$count file(s) uploaded")
      ()
    }
  }

  def pwd: List[AzureContainer] = {
    blobContainer.map { container =>
      AzureContainer(name = container.getName, uri = container.getUri.toASCIIString)
    } toList
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
  * Azure Blob Storage Companion
  * @author lawrence.daniels@gmail.com
  */
object AzureBlobStorage {

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

  case class AzureContainer(name: String, uri: String)

}
