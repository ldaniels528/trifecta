package com.ldaniels528.trifecta.messages.query

import com.ldaniels528.trifecta.TxRuntimeContext
import com.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.ldaniels528.trifecta.io.{AsyncIO, InputSource, OutputSource}
import com.ldaniels528.trifecta.messages.logic.ConditionCompiler._
import com.ldaniels528.trifecta.messages.logic.Expressions.Expression
import com.ldaniels528.trifecta.messages.{MessageCodecs, MessageDecoder}
import com.ldaniels528.commons.helpers.OptionHelper._

import scala.concurrent.ExecutionContext

/**
 * KQL Selection Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class KQLSelection(source: IOSource,
                        destination: Option[IOSource] = None,
                        fields: Seq[String],
                        criteria: Option[Expression],
                        limit: Option[Int])
  extends KQLQuery {

  /**
   * Executes the given query
   * @param rt the given [[TxRuntimeContext]]
   */
  override def executeQuery(rt: TxRuntimeContext)(implicit ec: ExecutionContext): AsyncIO = {
    val counter = IOCounter(System.currentTimeMillis())

    // get the input source and its decoder
    val inputSource: Option[InputSource] = rt.getInputHandler(rt.getDeviceURLWithDefault("topic", source.deviceURL))
    val inputDecoder: Option[MessageDecoder[_]] = source.decoderURL match {
      case None | Some("default") =>
        val topic = source.deviceURL.split("[:]").last
        rt.lookupDecoderByName(topic)
      case Some(decoderURL) =>
        rt.lookupDecoderByName(decoderURL) ?? MessageCodecs.getDecoder(decoderURL)
    }

    // get the output source and its encoder
    val outputSource: Option[OutputSource] = destination.flatMap(src => rt.getOutputHandler(src.deviceURL))
    val outputDecoder: Option[MessageDecoder[_]] = for {dest <- destination; url <- dest.decoderURL; decoder <- MessageCodecs.getDecoder(url)} yield decoder

    // compile conditions & get all other properties
    val conditions = criteria.map(compile(_, inputDecoder)).toSeq
    val maximum = limit ?? Some(25)

    // perform the query/copy operation
    val task = if (outputSource.nonEmpty) throw new IllegalStateException("Insert is not yet supported")
    else {
      val querySource = inputSource.flatMap(_.getQuerySource).orDie(s"No query compatible source found for URL '${source.deviceURL}'")
      val decoder = inputDecoder.orDie(s"No decoder found for URL ${source.decoderURL}")
      querySource.findAll(fields, decoder, conditions, maximum, counter)
    }

    AsyncIO(task, counter)
  }

  /**
   * Returns the string representation of the query
   * @example select symbol, exchange, lastTrade, open, close, high, low from "shocktrade.quotes.avro" with "avro:file:avro/quotes.avsc" where lastTrade <= 1 and volume >= 1,000,000
   * @example select strategy, groupedBy, vip, site, qName, srcIP, frequency from "dns.query.topHitters" with "avro:file:avro/topTalkers.avsc" where strategy == "IPv4-CMS" and groupedBy == "vip,site" limit 35
   * @example select strategy, groupedBy, vip, site, qName, srcIP, frequency from "dns.query.topHitters" with "avro:file:avro/topTalkers.avsc" where strategy == "IPv4-CMS"
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