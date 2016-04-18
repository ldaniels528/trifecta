package com.github.ldaniels528.trifecta.modules.etl.io.filters.impl

import java.text.SimpleDateFormat
import java.util.Date

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.filters.{Filter, FilterException}
import com.github.ldaniels528.commons.helpers.OptionHelper._

import scala.util.Try

/**
  * Date Filter
  * @author lawrence.daniels@gmail.com
  */
case class DateFilter() extends Filter {

  override def execute(value: Option[Any], args: List[String])(implicit scope: Scope) = {
    args match {
      case fx :: fxArgs if fx == "format" => formatDate(value, fxArgs)
      case fx :: fxArgs if fx == "parse" => parseDate(value, fxArgs)
      case fx :: fxArgs if fx == "to_millis" => toMillis(value, fxArgs)
      case fx :: _ =>
        throw new FilterException(this, s"Function $fx is unknown")
    }
  }

  private def formatDate(value: Option[Any], args: List[String]) = {
    value flatMap {
      case date: Date =>
        val format = args.headOption orDie "No date format specified"
        Try(new SimpleDateFormat(format).format(date)).toOption
      case v => Option(v)
    }
  }

  private def parseDate(value: Option[Any], args: List[String]) = {
    value flatMap {
      case dateString: String =>
        val format = args.headOption orDie "No date format specified"
        Try(new SimpleDateFormat(format).parse(dateString)).toOption
      case v => Option(v)
    }
  }

  private def toMillis(value: Option[Any], args: List[String]) = {
    value map {
      case date: Date => date.getTime
      case v => v
    }
  }

}
