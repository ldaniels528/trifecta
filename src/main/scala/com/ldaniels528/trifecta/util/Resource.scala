package com.ldaniels528.trifecta.util

import java.net.URL

import scala.language.reflectiveCalls

/**
 * Trifecta Resource
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Resource {

  def apply(path: String): Option[URL] = Option(getClass.getResource(path))

}
