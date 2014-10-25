package com.ldaniels528.trifecta.util

import scala.util.{Failure, Success, Try}

/**
 * Parsing Helper
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object ParsingHelper {

  def parseDelta(label: String, value: String): Int = {
    value.head match {
      case '+' => parseInt(label, value.tail)
      case _ => parseInt(label, value)
    }
  }

  def parseDouble(label: String, value: String): Double = {
    Try(value.toDouble) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an decimal value, found '$value'")
    }
  }

  def parseInt(label: String, value: String): Int = {
    Try(value.toInt) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an integer value, found '$value'")
    }
  }

  def parseLong(label: String, value: String): Long = {
    Try(value.toLong) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an long integer value, found '$value'")
    }
  }

  def parsePartition(partition: String): Int = parseInt("partition", partition)

  def parsePort(port: String): Int = parseInt("port", port)

  def parseOffset(offset: String): Long = parseLong("offset", offset)

}
