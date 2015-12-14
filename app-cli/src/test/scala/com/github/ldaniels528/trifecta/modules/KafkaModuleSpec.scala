package com.github.ldaniels528.trifecta.modules

import org.mockito.Mockito.{when => when$}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Kafka Module Specification
 * @author lawrence.daniels@gmail.com
 */
class KafkaModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen with MockitoSugar {
/*
val config = new TxConfig("host", 2181)
implicit val zk = mock[ZKProxy]
implicit val rt = new TxRuntimeContext(config, zk)


info("As a Kafka Module")
info("I want to be able to execute Kafka Module commands")

feature("The 'kbrokers' command provides a list of brokers from Zookeeper") {
  scenario("The command executes without parameters") {
    Given("a Kafka Module")
    val module = new KafkaModule(config)

    And("a Zookeeper environment")
    val ctx = new Object()
    val data = """{"controller_epoch":3,"leader":1,"version":1,"leader_epoch":0,"isr":[1]}""".getBytes("UTF-8")
    val paths = (0 to 4).map(n => s"/brokers/topics/test2.Shocktrade.quotes.avro/partitions/$n/state")

    When("the 'brokers' command is executed")
    when$(zk.getChildren("/brokers")).thenReturn(Seq[String]())
    val result = module.getBrokers(UnixLikeArgs(Option("/brokers"), Nil))

    Then("The tokens are transformed into Unix-style parameters")
    result.isEmpty shouldBe false
    Try(zk.close())
  }
}
  feature("Ability to perform a recursive delete of a path in Zookeeper (zrm -r))") {
    scenario("Recursively delete a path from Zookeeper") {
      Given("The initial Zookeeper environment")
      if (zk.exists("/consumers")) {
        zk.ensurePath("/consumers/otherTestId")
        zk.ensurePath("/consumers/myTestId/someSubPath")
      }
      if (zk.exists("/brokers")) zk.ensurePath("/brokers/ids")

      And("A Zookeeper key/path")
      val module = new ZookeeperModule(config)

      When("Executing the recursive delete function")
      module.delete(UnixLikeArgs(Some("delete"), Nil, Map("-r" -> Option("/consumers/myTestId"))))

      And("The path (and its children) should no longer exist")

      val results = zk.getChildren("/consumers")
      results shouldBe Seq("otherTestId")
      Try(zk.close())
    }
  } */

}
