package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.command.parser.TokenStream
import com.ldaniels528.trifecta.support.messaging.logic.ConditionCompiler._
import com.ldaniels528.trifecta.support.messaging.logic.Operations._

/**
 * Big Data Query Language Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object BigDataQueryParser {

  /**
   * Parses a BDQL query into a selection objects
   * @param queryString the given query string
   * @return the [[BigDataSelection]]
   */
  def parse(queryString: String): BigDataSelection = {
    // parse the query string
    val ts = TokenStream(BigDataQueryTokenizer.parse(queryString))

    /*
     * select symbol, exchange, lastTrade, volume
     * from kafka_quotes
     * into elastic_search_quotes
     * where exchange == 'OTCBB'
     * and lastTrade <= 1.0
     * and volume >= 1,000,000
     * limit 10
     */
    BigDataSelection(
      fields = parseSelectionFields(ts),
      source = parseFromExpression(ts),
      destination = parseIntoExpression(ts),
      criteria = parseWhereExpression(ts),
      limit = parseLimitExpression(ts))
  }

  /**
   * Parses the selection fields
   * @param ts the given [[TokenStream]]
   * @return the selection fields
   */
  private def parseSelectionFields(ts: TokenStream): Seq[String] = {
    ts.expect("select").getUntil(token = "from", delimiter = Option(","))
  }

  /**
   * Parses the "from" expression (e.g. "from kafka_quotes")
   * @param ts the given [[TokenStream]]
   * @return the option of an integer value
   */
  private def parseFromExpression(ts: TokenStream): String = {
    ts.expect("from").getOrElse(throw new IllegalArgumentException("Input source expected near 'from'"))
  }

  /**
   * Parses the "into" expression (e.g. "into elastic_search_quotes")
   * @param ts the given [[TokenStream]]
   * @return the option of an integer value
   */
  private def parseIntoExpression(ts: TokenStream): Option[String] = {
    ts.ifNext("into") {
      ts.getOrElse(throw new IllegalArgumentException("Output source expected near 'into'"))
    }
  }

  /**
   * Parses the "limit" expression (e.g. "limit 10")
   * @param ts the given [[TokenStream]]
   * @return the option of an integer value
   */
  private def parseLimitExpression(ts: TokenStream): Option[Int] = {
    ts.ifNext("limit") {
      ts.getOrElse(throw new IllegalArgumentException("Limit value expected near 'limit'")).toInt
    }
  }

  /**
   * Parses the "where" expression (e.g. "where price >= 5")
   * @param ts the given [[TokenStream]]
   * @return the option of an [[Operation]]
   */
  private def parseWhereExpression(ts: TokenStream): Option[Operation] = {
    ts.ifNext("where") {
      // where lastTrade >= 1 and volume >= 1,000,000
      var criteria: Option[Operation] = None
      val it = TokenStream(ts.getUntil("limit"))
      while (it.hasNext) {
        val args = if (criteria.isEmpty) it.take(3) else it.take(4)
        args match {
          case List("and", field, operator, value) => criteria = criteria.map(op => AND(op, compile(field, operator, value)))
          case List("or", field, operator, value) => criteria = criteria.map(op => OR(op, compile(field, operator, value)))
          case List(field, operator, value) => criteria = Option(compile(field, operator, value))
          case _ =>
            throw new IllegalArgumentException(s"Invalid expression near ${it.rewind(4).take(4).mkString(" ")}")
        }
      }
      criteria
    }.flatten
  }

}
