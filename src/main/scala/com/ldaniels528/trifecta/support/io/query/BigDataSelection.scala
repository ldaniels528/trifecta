package com.ldaniels528.trifecta.support.io.query

import com.ldaniels528.trifecta.decoders.MessageCodecs
import com.ldaniels528.trifecta.support.io.{InputSource, OutputSource}
import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.support.messaging.logic.ConditionCompiler._
import com.ldaniels528.trifecta.support.messaging.logic.Expressions.Expression
import com.ldaniels528.trifecta.util.TxUtils._
import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Big Data Selection Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class BigDataSelection(source: IOSource,
                            destination: Option[IOSource] = None,
                            fields: Seq[String],
                            criteria: Option[Expression],
                            limit: Option[Int]) {

  /**
   * Executes the selection query
   * @param rt the given runtime context
   */
  def execute(implicit config: TxConfig, rt: TxRuntimeContext, ec: ExecutionContext): Future[QueryResult] = {
    // get the input source and its decoder
    val inputSource: Option[InputSource] = rt.getInputHandler(source.deviceURL)
    val inputDecoder: Option[MessageDecoder[_]] = MessageCodecs.getDecoder(source.decoderURL)

    // get the output source and its encoder
    val outputSource: Option[OutputSource] = destination.flatMap(src => rt.getOutputHandler(src.deviceURL))
    val outputDecoder: Option[MessageDecoder[_]] = destination.flatMap(src => MessageCodecs.getDecoder(src.decoderURL))

    // compile conditions & get all other properties
    val conditions = criteria.map(compile(_, inputDecoder)).toSeq
    val maximum = limit ?? Some(25)

    // perform the query/copy operation
    if (outputSource.nonEmpty) throw new IllegalStateException("Insert is not yet supported")
    else {
      val querySource = inputSource.flatMap(_.getQuerySource).orDie(s"No query compatible source found for URL '${source.deviceURL}'")
      val decoder = inputDecoder.orDie(s"No decoder found for URL ${source.decoderURL}")
      querySource.findAll(fields, decoder, conditions, maximum)
    }
  }

  /**
   * Returns the string representation of the query
   * @example select symbol, exchange, lastTrade, open, close, high, low from "topic:shocktrade.quotes.avro" with "avro:file:avro/quotes.avsc" where lastTrade <= 1 and volume >= 1,000,000
   * @example select strategy, groupedBy, vip, site, qName, srcIP, frequency from "topic:dns.query.topHitters" with "avro:file:avro/topTalkers.avsc" where strategy == "IPv4-CMS" and groupedBy == "vip,site" limit 35
   * @example select strategy, groupedBy, vip, site, qName, srcIP, frequency from "topic:dns.query.topHitters" with "avro:file:avro/topTalkers.avsc" where strategy == "IPv4-CMS"
   * @return the string representation
   */
  override def toString = {
    val sb = new StringBuilder(s"select ${fields.mkString(", ")} from $source")
    destination.foreach(dest => sb.append(s" into $dest"))
    if (criteria.nonEmpty) {
      sb.append(" where ")
      sb.append(criteria.map(_.toString) mkString " ")
    }
    limit.foreach(count => sb.append(s" limit $count"))
    sb.toString()
  }

}

/**
 * Represents an Input/Output source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class IOSource(deviceURL: String, decoderURL: String) {
  override def toString = s"$deviceURL with $decoderURL"
}