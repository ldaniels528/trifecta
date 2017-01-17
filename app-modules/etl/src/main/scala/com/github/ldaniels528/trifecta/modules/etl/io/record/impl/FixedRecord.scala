package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet
import com.github.ldaniels528.trifecta.modules.etl.io.record.Field._
import com.github.ldaniels528.trifecta.modules.etl.io.record.{Field, Record, TextSupport}
import com.github.ldaniels528.commons.helpers.OptionHelper._

import scala.language.postfixOps

/**
  * Fixed Length Record implementation
  * @author lawrence.daniels@gmail.com
  */
case class FixedRecord(id: String, fields: Seq[Field]) extends Record with TextSupport {

  override def fromText(line: String)(implicit scope: Scope) = {
    var pos = 0
    DataSet(fields map { field =>
      val length = field.length getOrElse 1
      val value = extract(line, pos, pos + length).trim.convert(field.`type`)
      pos += length
      field.name -> value
    })
  }

  override def toText(dataSet: DataSet)(implicit scope: Scope) = {
    (fields zip dataSet.values(fields)).foldLeft[StringBuilder](new StringBuilder(length)) { case (sb, (field, (_, value))) =>
      val resolvedValue = value match {
        case Some(expr: String) => scope.evaluate(expr) ?? value
        case _ => value
      }
      val raw = resolvedValue.map(_.toString).getOrElse("")
      val length = field.length.getOrElse(raw.length)
      val sized = if (raw.length > length) raw.take(length) else raw + " " * (length - raw.length)
      sb.append(sized)
    } toString()
  }

  /**
    * Returns the record length
    * @return the record length
    */
  lazy val length = fields.map(_.length.getOrElse(1)).sum

  /**
    * Extracts a fixed-length portion of the given text
    * @param text  the given text
    * @param start the starting position of the substring
    * @param end   the ending position of the substring
    * @return a fixed-length portion
    */
  private def extract(text: String, start: Int, end: Int) = {
    if (start > text.length) " " * (end - start)
    else if (end > text.length) text.substring(start) + " " * ((end - start) - text.length)
    else text.substring(start, end)
  }

}
