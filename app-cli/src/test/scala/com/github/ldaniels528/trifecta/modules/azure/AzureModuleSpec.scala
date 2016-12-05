package com.github.ldaniels528.trifecta.modules.azure

import java.io.{File, FileInputStream}
import java.util.Properties

import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.messages.MessageSourceFactory
import com.github.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Azure Module Specification
  * @author lawrence.daniels@gmail.com
  */
class AzureModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {
  private val config = TxConfig.defaultConfig
  private val messageSourceFactory = new MessageSourceFactory()
  private implicit val zk = mock[ZKProxy]
  private implicit val rt = TxRuntimeContext(config, messageSourceFactory)(global)
  private val module = new AzureModule(config)

  info("As a Azure Module")
  info("I want to be able to utilize Azure cloud services")

  feature("Avro Schemas can be loaded into memory") {
    scenario("A file containing an Avro Schemas is loaded into memory") {
      Given("an Avro schema")

    }
  }

}
