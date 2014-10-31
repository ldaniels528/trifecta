package com.ldaniels528.trifecta.command

import com.ldaniels528.trifecta.command.BigDataQueryParser._

import scala.collection.mutable.ListBuffer

/**
 * Big Data Query Language Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class BigDataQueryParser(queryString: String) {
  private val parsers: Seq[ListBuffer[String] => Boolean] = Seq(
    parseDoubleQuotedSequence, parseSingleQuotedSequence, parseNumeric, parseAlphaNumeric, parseSymbols)
  private val ca = queryString.toCharArray
  private var pos = 0

  def parse(): List[String] = {
    val tokens = ListBuffer[String]()

    // skip over any initial whitespace
    skipWhiteSpace()

    while (hasNext) {
      // did we find a match?
      if (!parsers.exists(_(tokens)))
        throw new IllegalArgumentException( s"""Illegal argument near "${nextSpan(Math.max(pos - 10, 0)).take(20)}" at $pos""")

      // skip over any trailing whitespace
      skipWhiteSpace()
    }

    tokens.toList
  }

  private def hasNext: Boolean = pos < ca.length

  private def currentChar: Char = ca(pos)

  private def nextChar: Option[Char] = if (pos + 1 < ca.length) Option(ca(pos + 1)) else None

  private def nextSpan(start: Int): String = {
    val s = String.copyValueOf(ca, start, pos - start)
    //System.err.println(s"nextSpan: $s")
    s
  }

  private def isWhiteSpace: Boolean = WhiteSpace.contains(ca(pos))

  private def skipWhiteSpace(): Unit = while (hasNext && isWhiteSpace) pos += 1

  private def parseSequence(buf: ListBuffer[String], startCh: Char, endCh: Char): Boolean = {
    if (currentChar != startCh) false
    else {
      val start = pos
      pos += 1
      while (hasNext && currentChar != endCh) pos += 1

      if (hasNext && currentChar == endCh) {
        pos += 1
        buf += nextSpan(start)
        true
      }
      else false
    }
  }

  private def parseAlphaNumeric(tokens: ListBuffer[String]): Boolean = {
    if (!currentChar.isLetter) false
    else {
      val start = pos
      pos += 1
      while (hasNext && (currentChar.isLetterOrDigit || currentChar == '_')) pos += 1
      tokens += nextSpan(start)
      true
    }
  }

  private def parseDoubleQuotedSequence(tokens: ListBuffer[String]): Boolean = parseSequence(tokens, '"', '"')

  private def parseNumeric(tokens: ListBuffer[String]): Boolean = {
    if (!currentChar.isDigit) false
    else {
      val start = pos
      pos += 1
      while (hasNext && (currentChar.isDigit || currentChar == '.')) pos += 1
      tokens += nextSpan(start)
      true
    }
  }

  private def parseSingleQuotedSequence(tokens: ListBuffer[String]): Boolean = parseSequence(tokens, '\'', '\'')

  private def parseSymbols(tokens: ListBuffer[String]): Boolean = {
    if (!Symbols.contains(currentChar)) false
    else {
      val firstCh = currentChar
      val secondCh = nextChar
      val start = pos
      pos += 1

      secondCh foreach { nextCh =>
        val symbol = s"$firstCh$nextCh"
        if (Operators.contains(symbol)) pos += 1
      }
      tokens += nextSpan(start)
      true
    }
  }

}

/**
 * Big Data Query Language Parser
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object BigDataQueryParser {
  private val Symbols = ",=<>"
  private val Operators = Seq(">=", "<=", "<>")
  private val WhiteSpace = " \t\r\n"

  def parse(queryString: String): List[String] = {
    new BigDataQueryParser(queryString).parse()
  }

  sealed trait BdQLQuery

}
