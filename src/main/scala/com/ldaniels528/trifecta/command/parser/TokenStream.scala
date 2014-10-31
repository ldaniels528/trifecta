package com.ldaniels528.trifecta.command.parser

/**
 * Token Stream
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TokenStream(tokens: Seq[String]) {
  var pos = 0

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
  def expect(expectedToken: String): Unit = {
    if (!hasNext) throw new IllegalArgumentException("Unexpected end of statement")
    else {
      val actualToken = next()
      if (!actualToken.equalsIgnoreCase(expectedToken)) {
        throw new IllegalArgumentException(s"Expected '$expectedToken' near '$actualToken'")
      }
    }
  }

  /**
   * The given token must be the next token in the stream to avoid an error
   * @param expectedToken the given string token
   */
  def expectOrElse(expectedToken: String, otherwise: => Unit): Unit = {
    if (!hasNext) throw new IllegalArgumentException("Unexpected end of statement")
    else {
      val actualToken = next()
      if (actualToken.equalsIgnoreCase(expectedToken)) otherwise
    }
  }

  /**
   * Retrieves the next token or the default value
   * @param otherwise the default value
   * @return the next token or the default value
   */
  def getOrElse(otherwise: => String): String = if (hasNext) next() else otherwise

  /**
   * Retrieves all tokens up to the limit token or end-of-line is reached
   * @param token the given limit token
   * @param delimiter the optional delimiter
   * @return the extracted tokens
   */
  def getUntil(token: String, delimiter: Option[String] = None): Seq[String] = {
    // get the qualified sequence of tokens
    val index = tokens.indexOf(token)
    val subList = if (index != -1) tokens.slice(pos, index) else tokens

    // if a delimiter is specified, extract only the even values
    val list = delimiter.map { delim =>
      var isEven: Boolean = false
      subList.foldLeft[List[String]](Nil) { (items, item) =>
        isEven = !isEven
        if (isEven) item :: items else items
      }.reverse
    } getOrElse subList

    pos = index
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

}