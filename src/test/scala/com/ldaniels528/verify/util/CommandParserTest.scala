package com.ldaniels528.verify.util

import org.junit.Test
import CommandParser._
import org.junit.Assert
import org.slf4j.LoggerFactory

/**
 * Verify Command Parser Test
 * @author lawrence.daniels@gmail.com
 */
class CommandParserTest {
  val logger = LoggerFactory.getLogger(getClass())

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