package com.ldaniels528.verify

import java.io.File
import java.io.File.separator

import com.ldaniels528.verify.io.EndPoint

import scala.util.Properties.userHome

/**
 * Verify Shell Runtime Context
 * @author lawrence.daniels@gmail.com
 */
class VerifyShellRuntime(val props: java.util.Properties) {
  // the default state of the console is "alive"
  var alive = true
  var maxHistory = 100
  var historyFile = new File(s"$userHome${separator}.verify${separator}history.txt")

  // get the ZooKeeper host/port
  val zkEndPoint = EndPoint(props.getProperty("zookeeper"))

}

/**
 * Verify Shell Runtime Singleton
 * @author lawrence.daniels@gmail.com
 */
object VerifyShellRuntime {

  def getInstance(host: String): VerifyShellRuntime = {
    val p = new java.util.Properties()
    p.setProperty("zookeeper", s"$host:2181")
    new VerifyShellRuntime(p)
  }

}