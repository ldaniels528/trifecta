package com.github.ldaniels528.trifecta.messages.query.parser

import com.github.ldaniels528.trifecta.io.TokenStream
import com.github.ldaniels528.trifecta.messages.logic.ConditionCompiler._
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.query.{IOSource, KQLQuery, KQLRestrictions, KQLSelection}
import com.github.ldaniels528.trifecta.util.ParsingHelper._

import scala.util.Try

/**
  * Kafka Query Language Parser
  * @author lawrence.daniels@gmail.com
  */
object KafkaQueryParser {

  /**
    * Parses a KQL query into a selection objects
    * @param queryString the given query string
    * @return the [[KQLQuery executable query]]
    */
  def apply(queryString: String): KQLQuery = {
    // parse the query string
    val ts = TokenStream(KafkaQueryTokenizer.parse(queryString))

    /*
     * select symbol, exchange, lastTrade, volume, [limit]
     * from [shocktrade.quotes.avro] with [avro:file:avro/quotes.avsc]
     * into [es:/quotes/quote/AAPL] with json
     * where exchange == "OTCBB"
     * and lastTrade <= 1.0
     * and volume >= 1,000,000
     * using consumer myGroupId
     * limit 10
     */
    KQLSelection(
      fields = parseSelectionFields(ts),
      source = parseFromExpression(ts),
      destination = parseIntoExpression(ts),
      criteria = parseWhereExpression(ts),
      restrictions = parseRestrictions(ts),
      limit = parseLimitExpression(ts))
  }

  /**
    * Parses the selection fields
    * @param ts the given [[TokenStream]]
    * @return the selection fields
    */
  private def parseSelectionFields(ts: TokenStream): Seq[String] = {
    ts.expect("select").getUntil(token = "from", delimiter = Option(",")).map(_.cleanse)
  }

  /**
    * Parses the "from" expression (e.g. "from kafka_quotes with json")
    * @param ts the given [[TokenStream]]
    * @return an [[IOSource]]
    */
  private def parseFromExpression(ts: TokenStream): IOSource = {
    val deviceURL = ts.expect("from").orFail("Device URL expected near 'from'")
    val decoderURL = ts.ifNext("with")(_.orFail("Decoder URL expected near 'with'"))
    IOSource(deviceURL.cleanse, decoderURL.map(_.cleanse))
  }

  /**
    * Parses the "into" expression (e.g. "into elastic_search_quotes with json")
    * @param ts the given [[TokenStream]]
    * @return the option of an [[IOSource]]
    */
  private def parseIntoExpression(ts: TokenStream): Option[IOSource] = {
    ts.ifNext("into") { ts =>
      val deviceURL = ts.orFail("Output source expected near 'into'")
      val decoderURL = ts.ifNext("with")(_.orFail("Decoder URL expected near 'with'"))
      IOSource(deviceURL.cleanse, decoderURL.map(_.cleanse))
    }
  }

  /**
    * Parses the "limit" expression (e.g. "limit 10")
    * @param ts the given [[TokenStream]]
    * @return the option of an integer value
    */
  private def parseLimitExpression(ts: TokenStream): Option[Int] = {
    ts.ifNext("limit")(_.orFail("Limit value expected near 'limit'").toInt)
  }

  /**
    * Parses the "where" expression (e.g. "where price >= 5")
    * @param ts the given [[TokenStream]]
    * @return the option of an [[Expression]]
    */
  private def parseWhereExpression(ts: TokenStream): Option[Expression] = {
    ts.ifNext("where") { ts =>
      // where lastTrade >= 1 and volume >= 1,000,000
      var criteria: Option[Expression] = None
      val it = TokenStream(ts.getUntilAny("using", "limit"))
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

  /**
    * Parses the "using" expression (e.g. "using consumer test_dev")
    * @param ts the given [[TokenStream token stream]]
    * @return the collection of [[KQLRestrictions restrictions]]
    */
  private def parseRestrictions(ts: TokenStream): KQLRestrictions = {
    var restrictions = KQLRestrictions()
    var found = false
    do {
      found = ts.ifNext("using") { ts =>
        ts.next() match {
          case "consumer" => restrictions = restrictions.copy(groupId = Option(ts.next()))
          case "delta" => restrictions = restrictions.copy(delta = Try(ts.next().toLong).toOption)
          case unknown =>
            throw new IllegalArgumentException(s"Expected consumer or delta modifier in with clause near $unknown")
        }
      }.nonEmpty
    } while (found)
    restrictions
  }

  /**
    * Query Parsing Extensions
    * @param string the given string
    */
  final implicit class QueryParsingExtensions(val string: String) extends AnyVal {

    @inline
    def cleanse: String = string match {
      case s if s.startsWith("\"") && s.endsWith("\"") => s.drop(1).dropRight(1).trim
      case s if s.startsWith("'") && s.endsWith("'") => s.drop(1).dropRight(1).trim
      case s if s.startsWith("`") && s.endsWith("`") => s.drop(1).dropRight(1).trim
      case s if s.startsWith("[") && s.endsWith("]") => s.drop(1).dropRight(1).trim
      case s => s
    }
  }

}
