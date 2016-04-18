package com.github.ldaniels528.trifecta.messages.query

import com.github.ldaniels528.trifecta.TxRuntimeContext
import com.github.ldaniels528.trifecta.io.AsyncIO

import scala.concurrent.ExecutionContext

/**
 * Represents a Kafka Query Language (KQL) Question
 * @author lawrence.daniels@gmail.com
 */
trait KQLQuery {

  /**
   * Executes the given query
   * @param rt the given [[TxRuntimeContext]]
   */
  def executeQuery(rt: TxRuntimeContext)(implicit ec: ExecutionContext): AsyncIO

  /**
   * The query source (e.g. Kafka topic)
   * @return the query source
   */
  def source: IOSource

}
