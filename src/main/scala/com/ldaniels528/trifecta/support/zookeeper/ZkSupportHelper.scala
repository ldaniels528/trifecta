package com.ldaniels528.trifecta.support.zookeeper

/**
 * Zookeeper Support Helper Utility
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ZkSupportHelper {

  def zkExpandKeyToPath(key: String, zkCwd: String = "/"): String = {
    key match {
      case s if s.startsWith("/") => key
      case s => (if (zkCwd.endsWith("/")) zkCwd else zkCwd + "/") + s
    }
  }

  def zkKeyToPath(parent: String, child: String): String = {
    val parentWithSlash = if (parent.endsWith("/")) parent else parent + "/"
    parentWithSlash + child
  }

}
