package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.command.parser.bdql.BigDataQueryTokenizer._

import scala.collection.mutable.ListBuffer

/**
 * Big Data Query Language Tokenizer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class BigDataQueryTokenizer(queryString: String) {
  private val parsers: Seq[ListBuffer[Token] => Boolean] = Seq(
    parseDoubleQuotedSequence, parseSingleQuotedSequence, parseNumeric, parseAlphaNumeric, parseSymbols)
  private val ca = queryString.toCharArray
  private var pos = 0

  def parse(): List[Token] = {
    val tokens = ListBuffer[Token]()

    // skip over any initial whitespace
    skipWhiteSpace()

    while (hasNext) {
      // did we find a match?
      if (!parsers.exists(_(tokens)))
        throw new IllegalArgumentException( s"""Illegal argument near "${nextSpan(Math.max(pos - 10, 0)).word.take(20)}" at $pos""")

      // skip over any trailing whitespace
      skipWhiteSpace()
    }

    tokens.toList
  }

  private def hasNext: Boolean = pos < ca.length

  private def currentChar: Char = ca(pos)

  private def nextChar: Option[Char] = if (pos + 1 < ca.length) Option(ca(pos + 1)) else None

  private def nextSpan(start: Int): Token = Token(String.copyValueOf(ca, start, pos - start), start)

  private def isWhiteSpace: Boolean = WhiteSpace.contains(ca(pos))

  private def skipWhiteSpace(): Unit = while (hasNext && isWhiteSpace) pos += 1

  private def parseSequence(buf: ListBuffer[Token], startCh: Char, endCh: Char): Boolean = {
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

  private def parseAlphaNumeric(tokens: ListBuffer[Token]): Boolean = {
    if (!currentChar.isLetter) false
    else {
      val start = pos
      pos += 1
      while (hasNext && (currentChar.isLetterOrDigit || currentChar == '_')) pos += 1
      tokens += nextSpan(start)
      true
    }
  }

  private def parseDoubleQuotedSequence(tokens: ListBuffer[Token]): Boolean = parseSequence(tokens, '"', '"')

  private def parseNumeric(tokens: ListBuffer[Token]): Boolean = {
    if (!currentChar.isDigit) false
    else {
      val start = pos
      pos += 1
      while (hasNext && (currentChar.isDigit || currentChar == ',' || currentChar == '.')) pos += 1
      tokens += nextSpan(start)
      true
    }
  }

  private def parseSingleQuotedSequence(tokens: ListBuffer[Token]): Boolean = parseSequence(tokens, '\'', '\'')

  private def parseSymbols(tokens: ListBuffer[Token]): Boolean = {
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
 * Big Data Query Language Tokenizer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object BigDataQueryTokenizer {
  private val Symbols = "!,=<>"
  private val Operators = Seq(">=", "<=", "!=", "==")
  private val WhiteSpace = " \t\r\n"

  def parse(queryString: String): List[String] = {
    new BigDataQueryTokenizer(queryString).parse() map (_.word)
  }

  def parseWithPositions(queryString: String): List[Token] = {
    new BigDataQueryTokenizer(queryString).parse()
  }

  case class Token(word: String, pos: Int)
}
