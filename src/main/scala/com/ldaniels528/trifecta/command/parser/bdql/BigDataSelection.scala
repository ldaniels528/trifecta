package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.support.messaging.logic.Operations.Operation

/**
 * Big Data Selection Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class BigDataSelection(source: String,
                            destination: Option[String] = None,
                            fields: Seq[String],
                            criteria: Option[Operation],
                            limit: Option[Int]) {
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