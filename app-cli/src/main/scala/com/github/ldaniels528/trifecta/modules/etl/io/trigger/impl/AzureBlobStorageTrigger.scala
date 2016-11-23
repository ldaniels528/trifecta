package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.{File, FileInputStream, FileOutputStream}

import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.microsoft.azure.storage._
import com.microsoft.azure.storage.blob._

import scala.collection.JavaConversions._

/**
  * Azure Blob Storage Trigger
  * @author lawrence.daniels@gmail.com
  */
class AzureBlobStorageTrigger(connectionString: String, containerName: String) {
  private val storageAccount = CloudStorageAccount.parse(connectionString)
  private val blobClient = storageAccount.createCloudBlobClient()
  private val container = blobClient.getContainerReference(containerName.toLowerCase)

  def download(targetDirectory: File, fileName: String) = {
    container.listBlobs(fileName) foreach {
      case blob: CloudBlob =>
        new FileOutputStream(new File(targetDirectory, blob.getName)) use blob.download
      case blob =>
        throw new IllegalArgumentException(s"Unexpected blob type - ${blob.getClass.getName}")
    }
  }

  def downloadAll(targetDirectory: File): Unit = downloadAll(container, targetDirectory)

  private def downloadAll(container: CloudBlobContainer, targetDirectory: File) {
    container.listBlobs() foreach {
      case blob: CloudBlob =>
        new FileOutputStream(new File(targetDirectory, blob.getName)) use blob.download
      case directory: CloudBlobDirectory =>
        val subContainer = directory.getContainer
        downloadAll(subContainer, new File(targetDirectory, subContainer.getName))
      case blob =>
        throw new IllegalArgumentException(s"Unexpected blob type - ${blob.getClass.getName}")
    }
  }

  def ensureContainer = container.createIfNotExists()

  def list = container.listBlobs()

  def upload(srcFile: File, targetFileName: String) = {
    val blob = container.getBlockBlobReference(targetFileName)
    blob.upload(new FileInputStream(srcFile), srcFile.length())
  }

}

/**
  * Azure Blob Storage Trigger Companion Object
  */
object AzureBlobStorageTrigger {

  def apply(accountName: String, accountKey: String, containerName: String) = {
    val connectionString = s"DefaultEndpointsProtocol=http;AccountName=$accountName;AccountKey=$accountKey"
    new AzureBlobStorageTrigger(connectionString, containerName)
  }

  def apply(connectionString: String, containerName: String) = {
    new AzureBlobStorageTrigger(connectionString, containerName)
  }

}