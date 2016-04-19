package com.github.ldaniels528.commons.helpers

import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions

/**
 * Time Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object TimeHelper {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Executes the given block and writes the execution time (in milliseconds)
   * to the associated logger instance.
   * @param label the named label of the block
   * @param block the block to execute
   * @tparam T
   * @return the result of the execution
   */
  def time[T](label: String)(block: => T): T = {
    val start = System.nanoTime()
    val result = block
    val elapsed = (System.nanoTime() - start).toDouble / 1e+6
    logger.info(f"$label completed in $elapsed%.1f msec")
    result
  }

  object Implicits {

    /**
     * Syntactic sugar for timeout expressions
     */
    implicit def duration2Long(d: FiniteDuration): Long = d.toMillis

  }

}
