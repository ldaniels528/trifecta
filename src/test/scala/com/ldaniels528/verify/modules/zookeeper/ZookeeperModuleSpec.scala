package com.ldaniels528.verify.modules.zookeeper

import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

/**
 * Zookeeper Module Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ZookeeperModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen {
  private var zkTestServer: Option[TestingServer] = None

  override protected def beforeEach() {
    //zkTestServer = Option(new TestingServer(2181))
  }

  override protected def afterEach() {
    // shutdown the Zookeeper instance
    zkTestServer.foreach(_.close())
  }

  feature("Ability to perform a recursive delete of a path in Zookeeper)") {
    scenario("Recursively delete a path from Zookeeper") {
      Given("The initial Zookeeper environment")
      /*
      val zk = ZKProxy(EndPoint("localhost", 2181))
      zk.ensurePath("/brokers/ids")
      zk.ensurePath("/consumers/otherTestId")
      zk.ensurePath("/consumers/myTestId/someSubPath")

      Given("A Verify Run-time Context")
      val rt = VxRuntimeContext(zk)

      Given("A Zookeeper key/path")
      val module = new ZookeeperModule(rt)
      val path = "/consumers/myTestId"

      When("Executing the recursive delete function")
      module.delete(UnixLikeArgs(Nil, Map("-r" -> Option(path))))

      And("The path (and its children) should no longer exist")
      val results = rt.zkProxy.getChildren("/consumers")
      assert(results sameElements Seq("otherTestId"))
      rt.zkProxy.close()*/
    }
  }

}

