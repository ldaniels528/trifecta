package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.support.messaging.logic.Condition

/**
 * Big Data Selection Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class BigDataSelection(fields: List[String],
                            source: String,
                            conditions: Seq[Condition],
                            limit: Option[Int]) {
  override def toString = {
    val sb = new StringBuilder(s"select ${fields.mkString(", ")} from $source")
    if (conditions.nonEmpty) {
      sb.append(" where ")
      sb.append(conditions.map(_.toString) mkString " ")
    }
    limit.foreach(count => sb.append(s" limit $count"))
    sb.toString()
  }
}