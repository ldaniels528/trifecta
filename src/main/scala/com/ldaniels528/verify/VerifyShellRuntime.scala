package com.ldaniels528.verify

import java.io.File
import java.io.File.separator

import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.zookeeper.ZKProxy

import scala.util.Properties.userHome

/**
 * Verify Shell Runtime Context
 * @author lawrence.daniels@gmail.com
 */
case class VerifyShellRuntime(zkHost: String, zkPort: Int) {
  val remoteHost = s"$zkHost:$zkPort"

  // the default state of the console is "alive"
  var alive = true
  var debugOn = false
  var defaultFetchSize = 1024
  var encoding = "UTF-8"

  // define the history properties
  var maxHistory = 100
  var historyFile = new File(s"$userHome$separator.verify${separator}history.txt")

  // ZooKeeper current working directory
  var zkcwd = "/"

  // get the ZooKeeper host/port
  val zkEndPoint = EndPoint(zkHost, zkPort)

  // create the ZooKeeper proxy
  val zkProxy = ZKProxy(zkEndPoint)

}
