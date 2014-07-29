package com.ldaniels528.verify.util

import Tabular._

/**
 * Tabular
 * @author lawrence.daniels@gmail.com
 * @date 11/27/2013
 */
class Tabular() {
  import java.lang.Math.max
  import java.text.SimpleDateFormat
  import java.util.Date
  import scala.collection.JavaConversions._
  import scala.language.postfixOps
  import scala.util.{ Failure, Success, Try }

  /**
   * Transforms the given sequence of objects into a sequence of string that
   * represent a table.
   */
  def transform[A](values: Seq[A]): Seq[String] = {
    if (values.isEmpty) Seq.empty
    else {
      // get the headers, data rows, and column widths
      val headers = getHeaders(values(0))
      val rows = values map (convert(headers, _))

      // create the table
      makeTable(headers, rows)
    }
  }

  def transformPrimitives[A](values: Seq[A]): Seq[String] = {
    if (values.isEmpty) Seq.empty
    else {
      // get the headers, data rows, and column widths
      val headers = Seq("values")
      val rows = values map (v => Map(("values" -> asString(v))))

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
      val headers = values map (_._1) toSeq
      val rows = Seq(values) map (_ map { case (k, v) => (k, asString(v)) })

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
      val headers = values map (_._1) toSeq
      val rows = Seq(Map(values: _*)) map (_ map { case (k, v) => (k, asString(v)) })

      // create the table
      makeTable(headers, rows)
    }
  }

  protected def makeTable(headers: Seq[String], rows: Seq[Map[String, String]]): List[String] = {
    // create the horizontal border, header and compute column widths
    val widths = columnWidths(headers, rows)
    val borderLine = s"+ ${"-" * (widths.sum)} +"
    val headerLine = s"| ${constructRow(headers zip (widths))} |"

    // create the data grid
    val datagrid = (rows map { row =>
      val data = headers map (row.get(_) getOrElse " ")
      s"| ${constructRow(data zip (widths))} |"
    }).toList

    // create the table  
    borderLine :: headerLine :: borderLine :: datagrid ::: borderLine :: Nil
  }

  protected def convert[A](headers: Seq[String], v: A): Map[String, String] = {
    val beanClass = v.getClass
    Map(headers map (f => (f, asString(invokeMethod(v, f)))): _*)
  }

  protected def invokeMethod[A](v: A, f: String) = {
    import scala.util.{ Try, Success, Failure }
    val beanClass = v.getClass
    Try(beanClass.getMethod(f).invoke(v)) match {
      case Success(result) => result
      case Failure(e) =>
        System.err.println(s"Failed to invoke $f on ${beanClass.getName} - ${e.getMessage()}")
        ""
    }
  }

  protected def asString(value: Any): String = {
    import java.text.SimpleDateFormat
    import java.util.Date
    
    value match {
      case v if v == null => ""
      case b: Boolean => if (b) "Y" else "N"
      case d: Date => new SimpleDateFormat("MM/dd/yyyy hh:mma z").format(d)
      case o: Option[Any] => if (o.isDefined) asString(o.get) else ""
      case v => String.valueOf(v)
    }
  }

  protected def getHeaders[A](value: A): Seq[String] = {
    value.getClass.getDeclaredFields() map (_.getName()) filterNot (unwantedFields)
  }

  protected def constructRow(values: Seq[(String, Int)]): String = {
    (values map { case (data, width) => data + " " * Math.abs(width - data.length) }).mkString
  }

  /**
   * Computes the width of each column
   */
  protected def columnWidths(headers: Seq[String], rows: Seq[Map[String, String]]) = {
    import Math.max

    // define a function to compute the maximum length of the key-value pair
    def smash(k: String, v: String, currentMax: Int) = max(max(k.length, v.length), currentMax)

    // reduce the rows to a mapping of column to max width
    val result = rows.foldLeft[Map[String, Int]](Map.empty) {
      (res, row) =>
        res ++ (row map { case (k, v) => (k, smash(k, v, res.get(k) map (max(k.length, _)) getOrElse k.length)) })
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
  protected[this] val PRIMITIVES = Set[Class[_]](
    classOf[Byte], classOf[Char], classOf[Double], classOf[java.lang.Double], classOf[Float],
    classOf[Int], classOf[Long], classOf[Short], classOf[String])

  def isPrimitives[A](values: Seq[A]) = {
    if (values.isEmpty) true
    else {
      values(0) match {
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