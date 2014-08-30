package com.ldaniels528.verify

import com.ldaniels528.verify.VxConsole._
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Color._
import org.junit.{Assert, Test}
import org.slf4j.LoggerFactory

/**
 * Verify Console Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class VxConsoleTest {
  private val logger = LoggerFactory.getLogger(getClass)
  val VERSION = "0.10"

  @Test
  def testAnsiStringInterpolation(): Unit = {
    val string = a"${RED}Ve${GREEN}ri${BLUE}fy ${WHITE}v$VERSION"
    logger.info(s"string should an instance of ${classOf[Ansi].getName} (It is ${string.getClass.getName})")
    Assert.assertTrue(string.isInstanceOf[Ansi])
  }

}
