package com.github.ldaniels528.trifecta.modules.etl.io.record

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet

/**
  * Represents a generic data record
  */
trait Record {

  def id: String

  def fields: Seq[Field]

  lazy val fieldMapping = fields.map(field => field.name -> field)

  def asString(implicit scope: Scope) = {
    s"${getClass.getSimpleName}(${fields.map(f => s"${f.name}='${f.value}'").mkString(", ")})"
  }

  def toDataSet(implicit scope: Scope) = DataSet(fields map (field => (field.name, field.value)))

  def toFullString(implicit scope: Scope) = s"${getClass.getSimpleName}(${fields.map(f => s"${f.name}=${f.value}").mkString(", ")})"

  override def toString = s"${getClass.getSimpleName}(${fields.map(f => s"${f.name}=...").mkString(", ")})"

}
