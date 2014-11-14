package com.ldaniels528.trifecta.support.io

import java.net.URL

/**
 * Trifecta Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Resource {

  def apply(path: String): Option[URL] = Option(getClass.getResource(path))

  /**
   * Expands the UNIX path into a JVM-safe value
   * @param path the UNIX path (e.g. "~/ldaniels")
   * @return a JVM-safe value (e.g. "/home/ldaniels")
   */
  def expandPath(path: String): String = {
    path.replaceFirst("[~]", scala.util.Properties.userHome)
  }

}
