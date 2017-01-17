package com.github.ldaniels528.trifecta.util

import scala.util.{Failure, Success, Try}

/**
 * Parsing Helper
 * @author lawrence.daniels@gmail.com
 */
object ParsingHelper {

  def deQuote(quotedString: String): String = {
    quotedString match {
      case s if s.startsWith("`") && s.endsWith("`") => s.drop(1).dropRight(1)
      case s if s.startsWith("\"") && s.endsWith("\"") => s.drop(1).dropRight(1)
      case s if s.startsWith("'") && s.endsWith("'") => s.drop(1).dropRight(1)
      case s if s.contains(",") && s.replaceAll(",", "").matches("\\d+") => s.replaceAll(",", "")
      case s => s
    }
  }

  /**
    * Indicates whether the given string is hexadecimal dot-notation
    * @param value the given string value
    * @return true, if the string is hexadecimal dot-notation (e.g. "de.ad.be.ef.ca.fe.ba.be")
    */
  def isDottedHex(value: String): Boolean = {
    value.split("[.]").forall(_.matches( """[0-9a-fA-F]{2}"""))
  }

  /**
   * Parses the given input value and return its boolean equivalent
   * @param label the label of the value being parsed
   * @param value the given input value
   * @return the boolean equivalent value
   */
  def parseBoolean(label: String, value: String): Boolean = {
    value.toLowerCase match {
      case "true" | "yes" | "t" | "y" => true
      case "false" | "no" | "f" | "n" => false
      case invalid =>
        throw new IllegalArgumentException(s"$label: Expected a boolean value (true/false, yes/no, y/n, t/f) found '$invalid'")
    }
  }

  /**
   * Parses the given delta value (e.g. "+5")
   * @param label the label of the value being parsed
   * @param value the given input value
   * @return the equivalent integer value
   */
  def parseDelta(label: String, value: String): Int = {
    value.head match {
      case '+' => parseInt(label, value.tail)
      case _ => parseInt(label, value)
    }
  }

  /**
    * Converts a binary string to a byte array
    * @param dottedHex the given binary string (e.g. "de.ad.be.ef.00")
    * @return a byte array
    */
  def parseDottedHex(dottedHex: String): Array[Byte] = dottedHex.split("[.]") map (Integer.parseInt(_, 16)) map (_.toByte)

  /**
   * Parses the given integer value
   * @param label the label of the value being parsed
   * @param value the given input value
   * @return the equivalent integer value
   */
  def parseDouble(label: String, value: String): Double = {
    Try(value.toDouble) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected a decimal value, found '$value'")
    }
  }

  /**
   * Parses the given double value
   * @param label the label of the value being parsed
   * @param value the given input value
   * @return the equivalent double value
   */
  def parseInt(label: String, value: String): Int = {
    Try(value.toInt) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected an integer value, found '$value'")
    }
  }

  /**
   * Parses the given long integer value
   * @param label the label of the value being parsed
   * @param value the given input value
   * @return the equivalent long integer value
   */
  def parseLong(label: String, value: String): Long = {
    Try(value.toLong) match {
      case Success(v) => v
      case Failure(e) =>
        throw new IllegalArgumentException(s"$label: Expected a long integer value, found '$value'")
    }
  }

  /**
   * Parses the given partition string into an integer value
   * @param partition the given partition string value
   * @return the equivalent integer value
   */
  def parsePartition(partition: String): Int = parseInt("partition", partition)

  /**
   * Parses the given port string into an integer value
   * @param port the given port string value
   * @return the equivalent integer value
   */
  def parsePort(port: String): Int = parseInt("port", port)

  /**
   * Parses the given offset string into a long integer value
   * @param offset the given offset string value
   * @return the equivalent long integer value
   */
  def parseOffset(offset: String): Long = parseLong("offset", offset)

}
