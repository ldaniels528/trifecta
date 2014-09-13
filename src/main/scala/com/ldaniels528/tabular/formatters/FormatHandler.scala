package com.ldaniels528.tabular.formatters

/**
 * Tabular Data Formatter
 * @author lawrence.daniels@gmail.com
 */
trait FormatHandler {

  /**
   * Indicates whether this handler understands how to format the given value
   * @param value the given value
   * @return true, if the value is handled by this instance
   */
  def handles(value: Any): Boolean

  /**
   * Attempts to formats the given value
   * @param value the given value
   * @return an optional of a formatted value
   */
  def format(value: Any): Option[String]

}
