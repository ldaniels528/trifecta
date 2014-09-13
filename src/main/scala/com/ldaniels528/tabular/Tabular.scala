package com.ldaniels528.tabular

import com.ldaniels528.tabular.formatters.FormatHandler
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/**
 * Tabular
 * @author lawrence.daniels@gmail.com
 */
class Tabular() {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private var formatters: List[FormatHandler] = Nil

  /**
   * Attaches the given formatter to this instance
   * @param formatter the given format handler
   * @return self
   */
  def +=(formatter: FormatHandler): Tabular = {
    formatters = formatter :: formatters
    this
  }

  /**
   * Transforms the given sequence of objects into a sequence of string that
   * represent a table.
   */
  def transform[A](values: Seq[A]): Seq[String] = {
    if (values.isEmpty) Nil
    else {
      // get the headers, data rows, and column widths
      val headers = getHeaders(values.head)
      val rows = values map (convert(headers, _))

      // create the table
      makeTable(headers, rows)
    }
  }

  def transformPrimitives[A](values: Seq[A]): Seq[String] = {
    if (values.isEmpty) Nil
    else {
      // get the headers, data rows, and column widths
      val headers = Seq("values")
      val rows = values map (v => Map("values" -> asString(v)))

      // create the table
      makeTable(headers, rows)
    }
  }

  /**
   * Transforms the given mapping of objects into a sequence of string that
   * represent a table.
   */
  def transformMap[A](values: Map[String, A]): Seq[String] = {
    if (values.isEmpty) Nil
    else {
      // get the headers, data rows, and column widths
      val headers = (values map (_._1)).toSeq
      val rows = Seq(values) map (_ map { case (k, v) => (k, asString(v))})

      // create the table
      makeTable(headers, rows)
    }
  }

  /**
   * Transforms the given mapping of objects into a sequence of string that
   * represent a table.
   */
  def transformTuples(values: (String, Any)*): Seq[String] = {
    if (values.isEmpty) Nil
    else {
      // get the headers, data rows, and column widths
      val headers = (values map (_._1)).toSeq
      val rows = Seq(Map(values: _*)) map (_ map { case (k, v) => (k, asString(v))})

      // create the table
      makeTable(headers, rows)
    }
  }

  protected def makeTable(headers: Seq[String], rows: Seq[Map[String, String]]): List[String] = {
    // create the horizontal border, header and compute column widths
    val widths = columnWidths(headers, rows)
    val borderLine = s"+ ${"-" * widths.sum} +"
    val headerLine = s"| ${constructRow(headers zip widths)} |"

    // create the data grid
    val dataGrid = (rows map { row =>
      val data = headers map (row.getOrElse(_, " "))
      s"| ${constructRow(data zip widths)} |"
    }).toList

    // create the table
    borderLine :: headerLine :: borderLine :: dataGrid ::: borderLine :: Nil
  }

  protected def convert[A](headers: Seq[String], v: A): Map[String, String] = {
    Map(headers map (f => (f, asString(invokeMethod(v, f)))): _*)
  }

  protected def invokeMethod[A](v: A, f: String) = {
    import scala.util.{Failure, Success, Try}
    val beanClass = v.getClass
    Try(beanClass.getMethod(f).invoke(v)) match {
      case Success(result) => result
      case Failure(e) =>
        logger.error(s"Failed to invoke $f on ${beanClass.getName}", e)
        ""
    }
  }

  protected def asString(value: Any): String = {
    import java.text.SimpleDateFormat
    import java.util.Date

    formatters.find(_.handles(value)) flatMap(_.format(value)) match {
      case Some(formattedValue) => formattedValue
      case None =>
        value match {
          case v if v == null => ""
          case d: Date => new SimpleDateFormat("MM/dd/yy hh:mm:ss z").format(d)
          case o: Option[_] => if (o.isDefined) asString(o.get) else ""
          case v => String.valueOf(v)
        }
    }
  }

  protected def getHeaders[A](value: A): Seq[String] = {
    value.getClass.getDeclaredFields map (_.getName) filterNot unwantedFields
  }

  protected def constructRow(values: Seq[(String, Int)]): String = {
    (values map { case (data, width) => data + " " * Math.abs(width - data.length)}).mkString
  }

  /**
   * Computes the width of each column
   */
  protected def columnWidths(headers: Seq[String], rows: Seq[Map[String, String]]) = {
    import java.lang.Math.max

    // define a function to compute the maximum length of the key-value pair
    def smash(k: String, v: String, currentMax: Int) = max(max(k.length, v.length), currentMax)

    // reduce the rows to a mapping of column to max width
    val result = rows.foldLeft[Map[String, Int]](Map.empty) {
      (res, row) =>
        res ++ (row map { case (k, v) => (k, smash(k, v, res.get(k) map (max(k.length, _)) getOrElse k.length))})
    }

    // return just the column widths in the appropriate order
    headers map (result(_) + 2)
  }

  /**
   * Eliminates reflection artifacts
   */
  protected def unwantedFields(s: String) = Set("$outer").contains(s)

}

/**
 * Tabular (Companion Object)
 * @author lawrence.daniels@gmail.com
 */
object Tabular {

  def isPrimitives[A](values: Seq[A]) = {
    if (values.isEmpty) true
    else {
      values.head match {
        case b: Byte => true
        case d: Double => true
        case f: Float => true
        case i: Int => true
        case l: Long => true
        case n: Number => true
        case s: Short => true
        case s: String => true
        case _ => false
      }
    }
  }

}
