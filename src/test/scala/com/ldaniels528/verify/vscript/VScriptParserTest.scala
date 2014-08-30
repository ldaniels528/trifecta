package com.ldaniels528.verify.vscript

import com.ldaniels528.verify.vscript.VScriptParser.TokenIterator
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory

/**
 * VScript Parser Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VScriptParserTest {
  private val logger = LoggerFactory.getLogger(getClass)

  @Test
  def lists(): Unit = {
    implicit val tok = VScriptParser.parse(
      """
      	val a0 = [2, 3, 4]
      	println a0

      	val a1 = a0 :+ 5
      	println a1

    	  val a2 = a1 +: 1
      	println a2
      """,
      debug = true)

    checkLine(Seq("val", "a0", "=", "[", "2", ",", "3", ",", "4", "]", "\n"), "1st")
    checkLine(Seq("println", "a0", "\n"), "2nd")
    checkLine(Seq("val", "a1", "=", "a0", ":+", "5", "\n"), "3rd")
    checkLine(Seq("println", "a1", "\n"), "4th")
    checkLine(Seq("val", "a2", "=", "a1", "+:", "1", "\n"), "5th")
    checkLine(Seq("println", "a2", "\n"), "6th")
    ()
  }

  @Test
  def test1(): Unit = {
    implicit val tok = VScriptParser.parse(
      """
      	def doAdd(a, b) { a + b }

      	val y = 3
        val x = 2 * y + 5
        println doAdd(x,y)
      """,
      debug = true)

    checkLine(Seq("def", "doAdd", "(", "a", ",", "b", ")", "{", "a", "+", "b", "}", "\n"), "1st")
    checkLine(Seq("val", "y", "=", "3", "\n"), "2nd")
    checkLine(Seq("val", "x", "=", "2", "*", "y", "+", "5", "\n"), "3rd")
    checkLine(Seq("println", "doAdd", "(", "x", ",", "y", ")", "\n"), "4th")
    ()
  }

  private def checkLine(items: Seq[String], nth: String)(implicit tok: TokenIterator) = {
    logger.info(s"The $nth line should match expected results: [ ${items.map(mapper).mkString(", ")} ]")
    items foreach (Assert.assertEquals(_, tok.next))
  }

  private def mapper(token: String): String = token match {
    case "\n" => "<LF>"
    case "\r" => "<CR>"
    case s => s"'$s'"
  }

}
