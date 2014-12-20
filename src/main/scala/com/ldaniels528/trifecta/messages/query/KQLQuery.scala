package com.ldaniels528.trifecta.messages.query

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.io.AsyncIO

import scala.concurrent.ExecutionContext


/**
 * KQL Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
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
