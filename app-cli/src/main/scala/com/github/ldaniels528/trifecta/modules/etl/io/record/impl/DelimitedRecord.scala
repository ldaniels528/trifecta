package com.github.ldaniels528.trifecta.modules.etl.io.record.impl

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet
import com.github.ldaniels528.trifecta.modules.etl.io.record.Field._
import com.github.ldaniels528.trifecta.modules.etl.io.record.{Field, Record, TextSupport}

/**
  * Delimited Record
  * @author lawrence.daniels@gmail.com
  */
case class DelimitedRecord(id: String,
                           delimiter: Char,
                           isTextQuoted: Boolean = false,
                           isNumbersQuoted: Boolean = false,
                           fields: Seq[Field])
  extends Record with TextSupport {

  override def fromText(line: String)(implicit scope: Scope) = {
    val rawValues = fromDelimitedText(line)
    val delta = fields.size - rawValues.size
    val values = rawValues ++ (if (delta > 0) fields.takeRight(delta).map(_.defaultValue.getOrElse("")) else Nil)
    DataSet(fields zip values map {
      case (field, value: String) =>
        field.name -> value.convert(field.`type`)
      case (field, value) =>
        field.name -> Option(value)
    })
  }

  override def toText(dataSet: DataSet)(implicit scope: Scope) = {
    toDelimitedText(dataSet.values(fields) map {
      case (name, Some(value: String)) =>
        scope.evaluate(value) getOrElse value
      case (name, value) =>
        value.map(_.toString) getOrElse ""
    })
  }

  private def fromDelimitedText(text: String) = {
    var inQuotes = false
    val sb = new StringBuilder()
    val values = text.toCharArray.foldLeft[List[String]](Nil) {
      case (list, '"') =>
        inQuotes = !inQuotes
        list
      case (list, ch) if ch == delimiter =>
        if (inQuotes) {
          sb.append(ch)
          list
        }
        else {
          val s = sb.toString().trim
          sb.clear()
          s :: list
        }
      case (list, ch) =>
        sb.append(ch)
        list
    }
    (if (sb.nonEmpty) sb.toString().trim :: values else values).reverse
  }

  private def toDelimitedText(values: Seq[Any]) = {
    val text = values.foldLeft(new StringBuilder()) { (sb, value) =>
      sb.append(delimiter).append(quote(value))
    }
    text.toString().tail
  }

  private def quote(value: Any) = value match {
    case n@(_: BigDecimal | _: BigInt | _: Byte | _: Double | _: Float | _: Int | _: Long | _: Number) if !isNumbersQuoted => n.toString
    case s: String if isTextQuoted => s""""$s""""
    case x => x.toString
  }

}
