package com.github.ldaniels528.commons.helpers

/**
 * Path Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object PathHelper {

  /**
   * Expands the UNIX path into a JVM-safe value
   * @param path the UNIX path (e.g. "~/ldaniels")
   * @return a JVM-safe value (e.g. "/home/ldaniels")
   */
  def expandPath(path: String): String = {
    path.replaceFirst("[~]", scala.util.Properties.userHome)
  }

}
