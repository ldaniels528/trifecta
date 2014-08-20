package com.ldaniels528.verify

import java.io.{PrintStream, ByteArrayOutputStream, File}
import java.io.File.separator

import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.kafka.KafkaModule
import com.ldaniels528.verify.modules.{CoreModule, ModuleManager}
import com.ldaniels528.verify.modules.zookeeper.{ZookeeperModule, ZKProxy}

import scala.util.Properties.userHome

/**
 * Verify Shell Runtime Context
 * @author lawrence.daniels@gmail.com
 */
case class VerifyShellRuntime(zkHost: String, zkPort: Int) {
  val remoteHost = s"$zkHost:$zkPort"

  // redirect standard output and error to my own buffers
  val out = System.out
  val err = System.err
  val buffer = new ByteArrayOutputStream(16384)
  System.setOut(new PrintStream(buffer))

  // the default state of the console is "alive"
  var alive = true
  var debugOn = false
  var defaultFetchSize = 1024
  var encoding = "UTF-8"

  // define the history properties
  var historyFile = new File(s"$userHome$separator.verify${separator}history.txt")

  // get the ZooKeeper host/port
  val zkEndPoint = EndPoint(zkHost, zkPort)

  // create the ZooKeeper proxy
  val zkProxy = ZKProxy(zkEndPoint)

  // create the module manager
  val moduleManager = new ModuleManager()

  // load the modules
  moduleManager ++= Seq(
    new CoreModule(this),
    new KafkaModule(this),
    new ZookeeperModule(this))

}
