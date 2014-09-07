package com.ldaniels528.verify.modules

import com.ldaniels528.verify.modules.CommandParser._

/**
 * Verify Command Parser Test
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CommandParserTest() {
  import org.junit.{Assert, Test}
  import org.slf4j.LoggerFactory

  private val logger = LoggerFactory.getLogger(getClass())

  @Test
  def testSymbols() {
    logger.info("")
    logger.info("Testing line symbols:")

    val line = "!100"
    val toks = parse(line)

    val output = (1 to toks.size) zip toks
    output foreach {
      case (n, tok) =>
        logger.info(f"[$n%02d] tok: $tok")
    }

    Assert.assertTrue(toks.size == 2)
  }

  @Test
  def testSingle() {
    logger.info("")
    logger.info("Testing a single argument:")

    val line = "kls"
    val toks = parse(line)

    val output = (1 to toks.size) zip toks
    output foreach {
      case (n, tok) =>
        logger.info(f"[$n%02d] tok: $tok")
    }

    Assert.assertTrue(toks.size == 1)
  }

  @Test
  def testMultiple() {
    logger.info("")
    logger.info("Testing multiple arguments:")

    val line = "kdumpa avro/schema.avsc 9 1799020 a+b+c+d+e+f"
    val toks = parse(line)

    val output = (1 to toks.size) zip toks
    output foreach {
      case (n, tok) =>
        logger.info(f"[$n%02d] tok: $tok")
    }

    Assert.assertTrue(toks.size == 6)
  }

}