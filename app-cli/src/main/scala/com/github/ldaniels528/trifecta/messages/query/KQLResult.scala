package com.github.ldaniels528.trifecta.messages.query

/**
 * Represents a Query Result
 * @author lawrence.daniels@gmail.com
 */
case class KQLResult(topic: String, labels: Seq[String], values: Seq[Map[String, Any]], runTimeMillis: Double)