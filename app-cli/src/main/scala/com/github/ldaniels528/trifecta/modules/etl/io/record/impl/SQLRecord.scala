package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.record.{Record, Field}

/**
  * Represents a SQL Record
  * @author lawrence.daniels@gmail.com
  */
case class SQLRecord(id: String, fields: Seq[Field]) extends Record {

  def containsCondition = fields.exists(_.updateKey.contains(true))

}
