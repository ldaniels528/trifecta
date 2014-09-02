package com.ldaniels528.verify.io

import org.junit._
import org.slf4j.LoggerFactory

/**
 * EndPoint Tests
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EndPointTest {
  private val logger = LoggerFactory.getLogger(getClass)

  @Test
  def test(): Unit = {
    val host = "myHost"
    val port = 8080
    val endPoint = EndPoint(s"$host:$port")

    logger.info(s"The host should match the expected result ($host)")
    Assert.assertTrue(endPoint.host == host)

    logger.info(s"The port should match the expected result ($port)")
    Assert.assertTrue(endPoint.port == port)
  }

}
