package com.ldaniels528.trifecta.util

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
 * Time Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TimeHelper {

  /**
   * Syntactic sugar for timeout expressions
   */
  implicit def duration2Long(d: FiniteDuration): Long = d.toMillis

}
