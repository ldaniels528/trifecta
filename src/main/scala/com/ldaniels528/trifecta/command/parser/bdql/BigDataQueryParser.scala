package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.command.parser.TokenStream
import com.ldaniels528.trifecta.support.messaging.logic.Condition
import org.slf4j.LoggerFactory

/**
 * Big Data Query Language Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object BigDataQueryParser {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Parses a BDQL query into a selection objects
   * @param queryString the given query string
   * @return the [[BigDataSelection]]
   */
  def parse(queryString: String): BigDataSelection = {
    /*
     * select symbol, exchange, lastTrade, volume
     * from kafka_queries
     * where exchange = 'OTCBB'
     * and lastTrade <= 1.0
     * and volume >= 1000000
     * limit 10
     */
    val tokens = BigDataQueryTokenizer.parse(queryString)
    val ts = TokenStream(tokens)

    // expect: select <fields ...>
    ts.expect("select")
    val fields = ts.getUntil(token = "from", delimiter = Option(",")).toList

    // expect: from <source>
    ts.expect("from")
    val source = ts.getOrElse(throw new IllegalArgumentException("Query source expected"))

    // optional: where <conditions ..>
    val conditions = ts.ifNext("where") {
      ts.getUntil("limit").foldLeft[Seq[Condition]](Nil) { (criteria, token) =>
        criteria
      }
    }

    // optional: limit <count>
    val limit = ts.ifNext("limit") {
      ts.next().toInt
    }

    BigDataSelection(fields, source, conditions getOrElse Nil, limit)
  }

}
