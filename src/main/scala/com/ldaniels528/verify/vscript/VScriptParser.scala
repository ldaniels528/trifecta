package com.ldaniels528.verify.vscript

/**
 * VScript Parser
 * @author lawrence.daniels@gmail.com
 */
object VScriptParser {
  private[this] val logger = org.apache.log4j.Logger.getLogger(getClass)
  private[this] val compoundSymbols = Seq("===", "==", "::", ":+", "+:", "++", "--", "+=", "-=", "*=", "/=")
  private[this] val parsers = Seq[(Array[Char], Int) => (Option[String], Int)](
    skipWhiteSpace, parseEOL, parseDQuotes, parseSQuotes, parseComments,
    parseNumeric, parseAlphaNumeric, parseCompoundSymbol, parseSymbol)

  /**
   * Parse the source code into tokens
   */
  def parse(sourceCode: String, debug: Boolean = false): TokenIterator = {
    val ca = sourceCode.toArray
    val buf = collection.mutable.Buffer[String]()

    // skip the initial white space
    var p = 0
    while (p < ca.length && ca(p).isWhitespace) p += 1

    // parse the sequence of tokens
    while (p < ca.length) {
      // parse the next token
      val (tk, p0) = parsers.foldLeft[(Option[String], Int)]((None, p)) {
        case ((t_, p_), svc) => if (t_.isEmpty && p_ < ca.length) svc(ca, p_) else (t_, p_)
      }

      // add the token to the list
      tk match {
        case Some(s) => buf += s
        case None =>
      }
      p = if (p != p0) p0 else p + 1
    }

    if (debug) {
      logger.info(s"tokens =>\r ${
        buf map {
          case "\n" => "<LF>\n"
          case "\r" => "<CR>\r"
          case s => s"[$s]"
        } mkString " "
      }")
    }

    // return a token iterator
    new TokenIterator(buf)
  }

  def escapeTokens(s: String): String = {
    s match {
      case "\r" => "<CR>"
      case "\n" => "<LF>"
      case x => x
    }
  }

  def escapeTokens(op: Option[String]): String = {
    op match {
      case Some(s) if s == "\r" => "<CR>"
      case Some(s) if s == "\n" => "<LF>"
      case Some(s) => s
      case None => "<EOS>"
    }
  }

  private def parseComments(ca: Array[Char], pos: Int) = (None, parseStringSpan(ca, pos, "/*", "*/")._2)

  private def parseDQuotes(ca: Array[Char], pos: Int) = parseSpan(ca, pos, '"', '"')

  private def parseSQuotes(ca: Array[Char], pos: Int) = parseSpan(ca, pos, '\'', '\'')

  private def parseAlphaNumeric(ca: Array[Char], pos: Int) = parseSeq(ca, pos, c => c.isLetterOrDigit || c == '_')

  private def parseNumeric(ca: Array[Char], pos: Int) = parseSeq(ca, pos, _.isDigit)

  private def parseSymbol(ca: Array[Char], pos: Int) = (span(ca, pos, 1), pos + 1)

  private def parseEOL(ca: Array[Char], pos: Int): (Option[String], Int) = {
    // skip multiple carriage return and/or new line characters
    var p = pos
    while (p < ca.length && (ca(p) == '\r' || ca(p) == '\n')) p += 1

    // keep only the last one
    if (p > pos) tuple(ca, p - 1, p) else (None, p)
  }

  private def parseCompoundSymbol(ca: Array[Char], pos: Int): (Option[String], Int) = {
    def isMatch(op: String, p: Int) = (p + op.length < ca.length) && (op == String.copyValueOf(ca, p, op.length))
    compoundSymbols.foldLeft[(Option[String], Int)]((None, pos)) {
      case ((tok, p), op) => if (tok.isEmpty && isMatch(op, p)) tuple(ca, p, p + op.length) else (tok, p)
    }
  }

  private def parseSeq(ca: Array[Char], pos: Int, checker: Char => Boolean): (Option[String], Int) = {
    var p = pos
    while (p < ca.length && checker(ca(p))) {
      p += 1
    }
    if (p > pos) tuple(ca, pos, p) else (None, p)
  }

  private def parseSpan(ca: Array[Char], pos: Int, startSeq: Char, endSeq: Char): (Option[String], Int) = {
    if (ca(pos) != startSeq) (None, pos)
    else {
      var p = pos + 1
      while (p < ca.length && ca(p) != endSeq) {
        p += 1
      }
      if (p < ca.length) p += 1
      tuple(ca, pos, p)
    }
  }

  private def parseStringSpan(ca: Array[Char], pos: Int, startSeq: String, endSeq: String): (Option[String], Int) = {
    if (!isEqual(startSeq, ca, pos)) (None, pos)
    else {
      var p = pos + startSeq.length
      while (p < ca.length && !isEqual(endSeq, ca, p)) {
        p += 1
      }
      p += endSeq.length
      tuple(ca, pos, p)
    }
  }

  private def skipWhiteSpace(ca: Array[Char], pos: Int): (Option[String], Int) = {
    var p = pos
    while (p < ca.length && (ca(p) == ' ' || ca(p) == '\t')) p += 1
    (None, p)
  }

  private def isEqual(s: String, ca: Array[Char], pos: Int) = (pos + s.length < ca.length) && (s == String.copyValueOf(ca, pos, s.length))

  private def tuple(ca: Array[Char], start: Int, end: Int) = (span(ca, start, end - start), end)

  private def span(ca: Array[Char], pos: Int, length: Int) = Some(String.copyValueOf(ca, pos, length))

  /**
   * Token Iterator
   * @author lawrence.daniels@gmail.com
   */
  class TokenIterator(seq: Seq[String]) {
    private var position = 0

    def back: TokenIterator = {
      if (position > 0) position -= 1
      this
    }

    def forward: TokenIterator = {
      if (position < seq.length) position += 1
      this
    }

    def hasNext: Boolean = position < seq.length

    def hasNext(index: Int): Boolean = position + index < seq.length

    def next: String = {
      // check for unexpected end-of-file
      if (!hasNext)
        throw new IllegalStateException("Unexpected end-of-stream")

      val tok = seq(position)
      position += 1
      tok
    }

    def nextIf(values: String*): Option[String] = {
      peek match {
        case Some(s) if values.contains(s) => nextOption
        case _ => None
      }
    }

    def nextOption: Option[String] = {
      if (!hasNext) None else Some(next)
    }

    def previous: Option[String] = {
      if (position < 1) None
      else {
        position -= 1
        Some(seq(position))
      }
    }

    def expect(required: String): String = {
      val found = next
      if (found != required)
        throw new IllegalStateException(s"Expected '$required' found '$found'")
      found
    }

    def fx[T](required: String, tx: String => T): T = {
      val value = next
      if (value != required)
        throw new IllegalStateException(s"Expected '$required' found '$value'")
      tx(value)
    }

    def isNext(s: String): Boolean = {
      peek exists (_ == s)
    }

    def peek: Option[String] = {
      if (hasNext) Some(seq(position)) else None
    }

    def peek(index: Int): Option[String] = {
      if (hasNext(index)) Some(seq(position + index)) else None
    }

    def recall: Option[String] = {
      if (position > 0) Some(seq(position - 1)) else None
    }

    def remaining: Int = seq.length - position

    def reset() = position = 0

    def toSeq: Seq[String] = if (position < seq.length) seq.slice(position, seq.length) else Seq.empty

    override def toString = s"TokenIterator(current=${
      if (position > 0) {
        escapeTokens(seq(position - 1))
      }
      else "<EOS>"
    }, next=${escapeTokens(peek)}, position=$position)"
  }

}