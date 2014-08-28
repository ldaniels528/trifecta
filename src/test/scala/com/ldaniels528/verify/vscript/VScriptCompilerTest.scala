package com.ldaniels528.verify.vscript

import org.junit.Test
import org.slf4j.LoggerFactory

/**
 * VScript Compiler Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VScriptCompilerTest() {
  private val logger = LoggerFactory.getLogger(getClass)

  @Test
  def strings() {
    label("Strings")
    exec(
      """
    	def sayHello(name) {
    		println "Hello " :+ name :+ "!"
    		name
      }

      val result = sayHello("World")
      println "Result: " :+ result
      """,
      debug = true)
  }

  @Test
  def math() {
    label("Math")
    exec(
      """
      	def doAdd(a, b) { a + b }

      	val y = 3
        val x = 2 * y + 5
        println doAdd(x,y)
      """,
      debug = true)
  }

  @Test
  def lists() {
    label("Lists")
    exec(
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
  def factorial() {
    label("Factorial")
    exec(
      """
        def factorial(n) { if(n === 1) 1 else n * factorial(n-1) }
        val x = 5
        println "factorial(" :+ x :+ ") = " :+ factorial(x)
      """,
      debug = true)
  }

  @Test
  def objects() {
    label("Objects")
    exec(
      """
	  	class Math(arg) {
    		println "Argument = " :+ arg
    		def add(x,y) {
    			x + y
    		}
      }

      val m = new Math("testing")
      m::add(2,3)
      """,
      debug = true)
  }

  private def label(text: String) = {
    logger.info("=" * 40)
    logger.info(s"=\t\t$text")
    logger.info("=" * 40)
  }

  private def exec(script: => String, debug: Boolean = false) = {
    import com.ldaniels528.verify.vscript.VScriptCompiler._

    // compile the code
    val code = compile(script, debug)
    if (debug) logger.info(s"program = $code")

    // run the code
    if (debug) logger.info("Running the script...")
    val result = code.eval(RootScope())
    if (debug) logger.info(s"result = $result")
    println()
  }

}