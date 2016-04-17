package com.github.ldaniels528.trifecta.messages.query.parser

import com.github.ldaniels528.trifecta.command.parser.TokenStream
import com.github.ldaniels528.trifecta.messages.query.{KQLQuery, KQLSelection, IOSource}
import com.github.ldaniels528.trifecta.messages.logic.ConditionCompiler._
import com.github.ldaniels528.trifecta.messages.logic.Expressions._

/**
 * Kafka Query Language Parser
 * @author lawrence.daniels@gmail.com
 */
object KafkaQueryParser {

  /**
   * Parses a KQL query into a selection objects
   * @param queryString the given query string
   * @return the [[KQLQuery]]
   */
  def apply(queryString: String): KQLQuery = {
    // parse the query string
    val ts = TokenStream(KafkaQueryTokenizer.parse(queryString))

    /*
     * select symbol, exchange, lastTrade, volume
     * from "shocktrade.quotes.avro" with "avro:file:avro/quotes.avsc"
     * into "es:/quotes/quote/AAPL" with "json"
     * where exchange == 'OTCBB'
     * and lastTrade <= 1.0
     * and volume >= 1,000,000
     * limit 10
     */
    KQLSelection(
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
   * @return an [[IOSource]]
   */
  private def parseFromExpression(ts: TokenStream): IOSource = {
    val deviceURL = ts.expect("from").getOrElse(throw new IllegalArgumentException("Device URL expected near 'from'"))
    val decoderURL = ts.ifNext("with") {
      ts.getOrElse(throw new IllegalArgumentException("Decoder URL expected near 'with'"))
    } map deQuote
    IOSource(deQuote(deviceURL), decoderURL)
  }

  /**
   * Parses the "into" expression (e.g. "into elastic_search_quotes")
   * @param ts the given [[TokenStream]]
   * @return the option of an [[IOSource]]
   */
  private def parseIntoExpression(ts: TokenStream): Option[IOSource] = {
    ts.ifNext("into") {
      val deviceURL = ts.getOrElse(throw new IllegalArgumentException("Output source expected near 'into'"))
      val decoderURL = ts.ifNext("with") {
        ts.getOrElse(throw new IllegalArgumentException("Decoder URL expected near 'with'"))
      } map deQuote
      IOSource(deQuote(deviceURL), decoderURL)
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
   * @return the option of an [[Expression]]
   */
  private def parseWhereExpression(ts: TokenStream): Option[Expression] = {
    ts.ifNext("where") {
      // where lastTrade >= 1 and volume >= 1,000,000
      var criteria: Option[Expression] = None
      val it = TokenStream(ts.getUntil("limit"))
      while (it.hasNext) {
        val args = it.take(criteria.size + 3).toList
        criteria = args match {
          case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("and") => criteria.map(AND(_, compile(field, operator, deQuote(value))))
          case List(keyword, field, operator, value) if keyword.equalsIgnoreCase("or") => criteria.map(OR(_, compile(field, operator, deQuote(value))))
          case List(field, operator, value) => Option(compile(field, operator, deQuote(value)))
          case _ =>
            throw new IllegalArgumentException(s"Invalid expression near ${it.rewind(4).take(4).mkString(" ")}")
        }
      }
      criteria
    }.flatten
  }

  def deQuote(quotedString: String): String = {
    quotedString match {
      case s if s.startsWith("\"") && s.endsWith("\"") => s.drop(1).dropRight(1)
      case s if s.startsWith("'") && s.endsWith("'") => s.drop(1).dropRight(1)
      case s if s.contains(",") && s.replaceAll(",", "").matches("\\d+") => s.replaceAll(",", "")
      case s => s
    }
  }

}
