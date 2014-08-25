package com.ldaniels528.verify

import java.io.File
import java.io.File.separator

import com.ldaniels528.verify.io.EndPoint
import com.ldaniels528.verify.modules.kafka.KafkaModule
import com.ldaniels528.verify.modules.zookeeper.{ZKProxy, ZookeeperModule}
import com.ldaniels528.verify.modules.{CoreModule, ModuleManager}
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi._

import scala.util.Properties.userHome

/**
 * Verify Shell Runtime Context
 * @author lawrence.daniels@gmail.com
 */
case class VerifyShellRuntime(zkHost: String, zkPort: Int) {
  // capture standard output
  val out = System.out
  val err = System.err

  // the Zookeeper remote host
  val remoteHost = s"$zkHost:$zkPort"

  // get the ZooKeeper end-point
  val zkEndPoint = EndPoint(zkHost, zkPort)

  // the default state of the application is "alive"
  var alive = true

  // various shared state variables
  var autoSwitching = false
  var debugOn = false
  var defaultFetchSize = 1024
  var encoding = "UTF-8"

  // ZooKeeper current working directory
  var zkCwd = "/"

  // the number of columns to display when displaying bytes
  var columns = 25

  // define the history properties
  var historyFile = new File(s"$userHome$separator.verify${separator}history.txt")

  // create the ZooKeeper proxy
  val zkProxy = ZKProxy(zkEndPoint)

  // create the module manager
  val moduleManager = new ModuleManager()

  // load the modules
  moduleManager ++= Seq(
    new CoreModule(this),
    new KafkaModule(this),
    new ZookeeperModule(this))

  // set the zookeeper module as the "active" module
  moduleManager.findModuleByName("zookeeper") map { module =>
    moduleManager.setActiveModule(module)
  }

  /**
   * Displays the loaded configuration properties
   */
  def states(): Unit = {
    val myStates = Seq[(String, Any)](
      "core:  encoding" -> encoding,
      "core:  module auto-switching" -> autoSwitching,
      "core:  debugging" -> debugOn,
      "kafka: fetch size" -> defaultFetchSize
    )

    VxConsole.wrap {
      for ((title, state) <- myStates) {
        val (value, color) = state match {
          case v: Boolean => if (v) ("On", GREEN) else ("Off", YELLOW)
          case v: String => (v, CYAN)
          case v => (v.toString, MAGENTA)
        }
        out.println(ansi().fg(WHITE).a(s"[*] $title is ").fg(color).a(value).reset())
      }
    }
  }

}
