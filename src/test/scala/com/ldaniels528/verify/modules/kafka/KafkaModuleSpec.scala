package com.ldaniels528.verify.modules.kafka

import com.ldaniels528.verify.VxRuntimeContext
import com.ldaniels528.verify.modules.CommandParser.UnixLikeArgs
import com.ldaniels528.verify.modules.zookeeper.ZookeeperModule
import com.ldaniels528.verify.support.zookeeper.ZKProxy
import org.apache.curator.test.TestingServer
import org.scalatest.{BeforeAndAfterEach, FeatureSpec, GivenWhenThen}

import scala.util.Try

/**
 * Kafka Module Specification
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class KafkaModuleSpec() extends FeatureSpec with BeforeAndAfterEach with GivenWhenThen {
  //val zkTestServer = new TestingServer(2181)
  //val zk = ZKProxy("localhost", 2181)

  info("As a Kafka Module")
  info("I want to be able to execute Kafka Module commands")

  /*
  feature("The 'kbrokers' command provides a list of brokers from Zookeeper") {
    scenario("The command executes without parameters") {
      Given("A string containing Unix-style parameters with flags")
      val line = "kget -a schema -f outfile.txt shocktrades.quotes.csv 0 165 -b"

      When("The string is parsed into tokens")
      val tokens = CommandParser.parse(line)

      Then("The tokens are transformed into Unix-style parameters")
      val result = CommandParser.parseUnixLikeArgs(tokens)

      And("Finally validate the Unix-style parameters")
      result shouldBe UnixLikeArgs(List("kget", "shocktrades.quotes.csv", "0", "165"), Map("-f" -> Some("outfile.txt"), "-a" -> Some("schema"), "-b" -> None))
    }
  }*/

  feature("Ability to perform a recursive delete of a path in Zookeeper (zrm -r))") {
    scenario("Recursively delete a path from Zookeeper") {
      Given("The initial Zookeeper environment")
      /*
      if (zk.exists("/consumers").isEmpty) {
        zk.ensurePath("/consumers/otherTestId")
        zk.ensurePath("/consumers/myTestId/someSubPath")
      }
      if (zk.exists("/brokers").isEmpty) zk.ensurePath("/brokers/ids")

      Given("A Verify Run-time Context")
      val rt = VxRuntimeContext(zk)

      Given("A Zookeeper key/path")
      val module = new ZookeeperModule(rt)

      When("Executing the recursive delete function")
      module.delete(UnixLikeArgs(Nil, Map("-r" -> Option("/consumers/myTestId"))))

      And("The path (and its children) should no longer exist")
      val results = rt.zkProxy.getChildren("/consumers")
      assert(results sameElements Seq("otherTestId"))
      rt.zkProxy.close()*/
    }
  }

  // all tests are complete, close resources
  //Try(zk.close())
  //Try(zkTestServer.close())

}
