package com.github.ldaniels528.trifecta.io.zookeeper

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.test.TestingServer
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Try

/**
 * ZooKeeper Proxy
 * @author lawrence.daniels@gmail.com
 */
class ZKProxySpec() extends FeatureSpec with GivenWhenThen {
  var server: TestingServer = _
  var client: CuratorFramework = _

  info("As a ZooKeeper Proxy instance")
  info("I want to be able to execute ZooKeeper commands")

  feature("The Proxy can create data in a Zookeeper instance") {
    scenario("The Proxy inserts a data set into Zookeeper") {
      try {
        Given("a Zookeeper instance")
        server = new TestingServer(2181)

        And("a Zookeeper Proxy client")
        val zk = ZKProxy(server.getConnectString)

        And("a data set to insert")
        val basePath = "/brokers/topics/test2/partitions"
        val data = """{"controller_epoch":3,"leader":1,"version":1,"leader_epoch":0,"isr":[1]}""".getBytes("UTF-8")

        When("the data is inserted via the create command")
        zk.ensurePath(basePath)
        (0 to 4).foreach { n =>
          zk.create(s"/brokers/topics/test2/partitions/$n", new Array[Byte](0))
          zk.create(s"/brokers/topics/test2/partitions/$n/state", data)
        }

        Then("results should match the expected values")
        val results = zk.getFamily("/brokers")
        results foreach (line => info(s"result: $line"))
        results shouldBe Seq(
          "/brokers",
          "/brokers/topics",
          "/brokers/topics/test2",
          "/brokers/topics/test2/partitions",
          "/brokers/topics/test2/partitions/3",
          "/brokers/topics/test2/partitions/3/state",
          "/brokers/topics/test2/partitions/2",
          "/brokers/topics/test2/partitions/2/state",
          "/brokers/topics/test2/partitions/1",
          "/brokers/topics/test2/partitions/1/state",
          "/brokers/topics/test2/partitions/0",
          "/brokers/topics/test2/partitions/0/state",
          "/brokers/topics/test2/partitions/4",
          "/brokers/topics/test2/partitions/4/state")

      } finally {
        Try(client.close())
        Try(server.stop())
        ()
      }
    }
  }

}