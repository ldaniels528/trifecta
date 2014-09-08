package com.ldaniels528.verify.modules.zookeeper

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.util.VxUtils._
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryOneTime
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Zookeeper Module Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen {
  private var zkTestServer: Option[TestingServer] = _
  private var cli: Option[CuratorFramework] = _

  override protected def beforeEach() {
    zkTestServer = Option(new TestingServer(2181))
    cli = zkTestServer.map { zkServer =>
      CuratorFrameworkFactory.newClient(zkServer.getConnectString, new RetryOneTime(2000))
    }

    // ensure our required zookeeper keys exist
    new ZKProxy("localhost", 2181) use { zk =>
      zk.ensurePath("/brokers/ids")
      zk.ensurePath("/consumers/otherTestId")
      zk.ensurePath("/consumers/myTestId/someSubPath")
    }
  }

  override protected def afterEach() {
    // shutdown the Zookeeper instance
    cli.foreach(_.close())
    zkTestServer.foreach(_.stop())
  }

  feature("Ability to perform a recursive delete of a path in Zookeeper)") {
    scenario("Recursively delete a path from Zookeeper") {
      Given("A Zookeeper key/path")
      val rt = new VxRuntimeContext("localhost", 2181)
      val module = new ZookeeperModule(rt)
      val path = "/consumers/myTestId"

      When("When executing the delete function")
      module.delete("-r", path)

      Then("The path should no longer exist")
      val results = rt.zkProxy.getChildren("/consumers")
      assert(results sameElements Seq("otherTestId"))
    }
  }

}

