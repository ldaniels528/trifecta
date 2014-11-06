package com.ldaniels528.trifecta.support.io.query

/**
 * Represents a Query Result
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class QueryResult(labels: Seq[String], values: Seq[Map[String, Any]])