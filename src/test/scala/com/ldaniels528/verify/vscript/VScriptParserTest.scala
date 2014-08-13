package com.ldaniels528.verify.vscript

import org.junit.Test
import org.slf4j.LoggerFactory

/**
 * VScript Parser Test
 * @author lawrence.daniels@gmail.com
 */
class VScriptParserTest {
  private val logger = LoggerFactory.getLogger(getClass)

  @Test
  def lists(): Unit = {
    VScriptParser.parse(
      """
      	val a0 = [2, 3, 4]
      	println a0

      	val a1 = a0 :+ 5
      	println a1

    	  val a2 = a1 +: 1
      	println a2
      """,
      debug = true)
  }

  @Test
  def test1(): Unit = {
    VScriptParser.parse(
    """
      	def doAdd(a, b) { a + b }

      	val y = 3
        val x = 2 * y + 5
        println doAdd(x,y)
    """,
    debug = true)
  }

}
