package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.command.parser.TokenStream
import com.ldaniels528.trifecta.support.messaging.logic.ConditionCompiler._
import com.ldaniels528.trifecta.support.messaging.logic.Operations.Operation

import scala.collection.mutable

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
    /*
     * select symbol, exchange, lastTrade, volume
     * from kafka_quotes
     * into elastic_search_quotes
     * where exchange == 'OTCBB'
     * and lastTrade <= 1.0
     * and volume >= 1,000,000
     * limit 10
     */
    val ts = TokenStream(BigDataQueryTokenizer.parse(queryString))

    // expect: select <fields ...>
    ts.expect("select")
    val fields = ts.getUntil(token = "from", delimiter = Option(",")).toList

    // expect: from <source>
    ts.expect("from")
    val source = ts.getOrElse(throw new IllegalArgumentException("Input source expected near 'from'"))

    // optional: into <destination>
    val destination = ts.ifNext("into") {
      ts.getOrElse(throw new IllegalArgumentException("Output source expected near 'into'"))
    }

    // optional: where <conditions ..>
    val conditions = ts.ifNext("where") {
      // where lastTrade >= 1 and volume >= 1,000,000
      val criteria = mutable.ListBuffer[Operation]()
      val it = TokenStream(ts.getUntil("limit"))
      while (it.hasNext) {
        val args = if(criteria.isEmpty) it.take(3) else it.take(4)
        args match {
          case List("and", field, operator, value) => criteria += compile(field, operator, value)
          case List(field, operator, value) => criteria += compile(field, operator, value)
          case _ =>
            throw new IllegalArgumentException(s"Invalid expression near ${it.rewind(3).take(3).mkString(" ")}")
        }
      }
      criteria.toList
    }

    // optional: limit <count>
    val limit = ts.ifNext("limit") {
      ts.getOrElse(throw new IllegalArgumentException("Limit value expected near 'limit'")).toInt
    }

    BigDataSelection(source, destination, fields, conditions getOrElse Nil, limit)
  }

}
