package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import java.io.{File, FileInputStream}
import java.util.Properties

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

import scala.collection.JavaConversions._

/**
  * Azure Blob Storage Trigger Spec
  * @author lawrence.daniels@gmail.com
  */
class AzureBlobStorageTriggerSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {
  val connectionString = getConnectionString orDie "Property 'blobstorage.secret.connect' could not be retrieved"

  info("As a AzureBlobStorageTrigger instance")
  info("I want to be able to create, list, download and upload blobs")

  feature("Upload and download blobs") {
    scenario("Upload a blob to an existing storage account") {
      Given("a AzureBlobStorageTrigger")
      val helper = AzureBlobStorageTrigger(connectionString = connectionString, containerName = "test")

      And("a source file and destination path")
      val srcFile = new File("./app-cli/src/test/resources/etl/files/AMEX.txt")
      val destFile = new File("./app-cli/src/test/resources/etl/test/AMEX.txt")
      info(s"Source: ${srcFile.getAbsolutePath}")
      info(s"Destination: ${destFile.getAbsolutePath}")

      When("I upload a file...")
      helper.upload(srcFile = srcFile, targetFileName = "AMEX.txt")

      And("Download the same file...")
      destFile.getParentFile.mkdirs()
      helper.downloadAll(targetDirectory = destFile.getParentFile)

      Then("the file sizes should be identical")
      srcFile.length() shouldBe destFile.length()

      And("The files should be deleted")
      destFile.delete() && destFile.getParentFile.delete() shouldBe true
    }
  }

  feature("List blobs") {
    scenario("List cloud blobs from an existing storage account") {
      Given("a AzureBlobStorageTrigger")
      val helper = AzureBlobStorageTrigger(connectionString = connectionString, containerName = "test")

      When("I request the list of blobs...")
      helper.list foreach { blob =>
        info(s"blob: ${blob.getUri.toASCIIString}")
      }
    }
  }

  private def getConnectionString = {
    val props = new Properties()
    new FileInputStream(new File(new File(System.getProperty("user.home")), "connection.properties")) use props.load
    Option(props.getProperty("blob.storage.secret.connect"))
  }

}
