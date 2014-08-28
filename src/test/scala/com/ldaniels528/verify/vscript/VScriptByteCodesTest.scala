package com.ldaniels528.verify.vscript

import java.io.File

import com.ldaniels528.verify.vscript.VScriptByteCodeUtil._
import com.ldaniels528.verify.vscript.VScriptCompiler._
import org.junit.Test
import org.slf4j.LoggerFactory

/**
 * VScript Byte Codes Test Suite
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VScriptByteCodesTest {
  private val logger = LoggerFactory.getLogger(getClass())

  //@Test
  def compileDecompile() {
    // define the script
    val script = """
        val fx = def getQuote(symbol) { println symbol }
        getQuote("AAPL")
        fx("MSFT")
        val y = 0
        val x = 2 * y + 5
        println x + y
        val a0 = [1, 2, 3, 4, 5]
        val a1 = a0 :+ 6
        val a2 = a1 +: 0
        println a2
        getContestByName("Millionaire's Club")
                 """
    logger.info(s"script[${script.length} / ${compress(script.getBytes()).length} compressed] => $script")

    // compile the code
    val code = compile(script)
    logger.info("Executing compiled code...")
    code.eval(RootScope())

    // generate the byte code
    val bytes = compileByteCode(code)
    logger.info(s"${bytes.length} byte(s) generated")

    val decomped = decompileByteCode(bytes)
    logger.info(s"decomped = $decomped")

    logger.info("Executing decompiled code...")
    decomped.eval(RootScope())
    ()
  }

  @Test
  def decompileFile() {
    val code = VScriptByteCodeUtil.loadClass(new File("./src/test/resources/quotes.qsc"))
    logger.info(s"code = $code")
  }

}