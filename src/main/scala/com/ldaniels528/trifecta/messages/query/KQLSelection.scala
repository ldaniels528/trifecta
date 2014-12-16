package com.ldaniels528.trifecta.messages.query

import com.ldaniels528.trifecta.messages.logic.Expressions.Expression

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

/**
 * Represents an Input/Output source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class IOSource(deviceURL: String, decoderURL: String) {
  override def toString = s"$deviceURL with $decoderURL"
}