package com.github.ldaniels528.trifecta.command.parser

/**
 * Token Stream
 * @author lawrence.daniels@gmail.com
 */
case class TokenStream(tokens: Seq[String]) {
  private var pos = 0

  /**
   * Returns an option of a token at the given index
   * @param index the given index
   * @return the option of a token at the given index
   */
  def apply(index: Int): Option[String] = if (index < tokens.size) Option(tokens(index)) else None

  /**
   * Indicates whether the stream contains at least one instance of the given token
   * @param token the given string token
   * @return true, if the stream contains at least one instance of the token
   */
  def contains(token: String): Boolean = tokens.indexOf(token, pos) != -1

  /**
   * The given token must be the next token in the stream to avoid an error
   * @param expectedToken the given string token
   */
  def expect(expectedToken: String): TokenStream = {
    if (!hasNext) throw new IllegalArgumentException(s"Unexpected end of statement: expected '$expectedToken'")
    else {
      val actualToken = next()
      if (!actualToken.equalsIgnoreCase(expectedToken)) {
        throw new IllegalArgumentException(s"Expected '$expectedToken' near '$actualToken'")
      }
      this
    }
  }

  /**
   * The given token must be the next token in the stream to avoid an error
   * @param expectedToken the given string token
   */
  def expectOrElse(expectedToken: String, otherwise: => Unit): TokenStream = {
    if (!hasNext) throw new IllegalArgumentException(s"Unexpected end of statement: expected '$expectedToken'")
    else {
      val actualToken = next()
      if (actualToken.equalsIgnoreCase(expectedToken)) otherwise
      this
    }
  }

  /**
   * Retrieves the option of the next token in the stream
   * @return the option of the next token
   */
  def get: Option[String] = if (hasNext) Option(next()) else None

  /**
   * Retrieves the next token or the default value
   * @param otherwise the default value
   * @return the next token or the default value
   */
  def getOrElse(otherwise: => String): String = get.getOrElse(otherwise)

  /**
   * Retrieves all tokens up to the limit token or end-of-line is reached
   * @param token the given limit token
   * @param delimiter the optional delimiter
   * @return the extracted tokens
   */
  def getUntil(token: String, delimiter: Option[String] = None): Seq[String] = {
    // get the qualified sequence of tokens
    val index = tokens.map(_.toLowerCase).indexOf(token.toLowerCase)
    val limit = if (index != -1) index else tokens.size
    val subList = tokens.slice(pos, limit)

    // if a delimiter is specified, extract only the even values
    val list = delimiter.map { delim =>
      var isEven: Boolean = false
      subList.foldLeft[List[String]](Nil) { (items, item) =>
        isEven = !isEven
        if (isEven) item :: items else items
      }.reverse
    } getOrElse subList

    pos = limit
    list
  }

  /**
   * Indicates whether at least one more token exists
   * @return true, if at least one more token exists
   */
  def hasNext: Boolean = pos < tokens.length

  /**
   * If the next token in the stream matches the given token, the block is invoked and returns
   * an option of the value returned by block
   * @param token the given token
   * @param block the given block of execution
   * @tparam S the return type of the block
   * @return an option of the value returned by block
   */
  def ifNext[S](token: String)(block: => S): Option[S] = {
    if (hasNext && peek.exists(_.equalsIgnoreCase(token))) {
      pos += 1
      Option(block)
    }
    else None
  }

  /**
   * Returns the option of the index of the next occurrence of the given token
   * @param token the token being searched for
   * @return the option of the index of the next occurrence of the given token
   */
  def indexOf(token: String): Option[Int] = {
    tokens.indexOf(token, pos) match {
      case -1 => None
      case index => Some(index + pos)
    }
  }

  /**
   * Returns the next token in the stream or an error if none exists
   * @return the next token in the stream or an error if none exists
   */
  def next(): String = {
    if (!hasNext) throw new IllegalStateException(s"End of statement reached")
    else {
      val value = tokens(pos)
      pos += 1
      value
    }
  }

  /**
   * Returns the next token in the stream (if one exists) without moving the cursor
   * @return the next token in the stream
   */
  def peek: Option[String] = if (hasNext) Option(tokens(pos)) else None

  /**
   * Returns the cursor's position within the stream
   * @return the cursor's position
   */
  def position: Int = pos

  /**
   * Rewinds the cursor back by the given count
   * @param count the given count
   * @return this [[TokenStream]] instance
   */
  def rewind(count: Int): TokenStream = {
    if(pos > count) pos -= count else pos = 0
    this
  }

  /**
   * Retrieves the count number of tokens if possible
   * @param count the number of tokens desired
   * @return the sequence of tokens
   */
  def take(count: Int): Seq[String] = (1 to count).flatMap(n => get)

}