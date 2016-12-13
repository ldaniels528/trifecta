package com.github.ldaniels528.trifecta.messages.codec.apache

import org.junit.{Assert, Test}

import scala.util.Success

/**
  * Apache Access Log Decoder Spec
  * @author lawrence.daniels@gmail.com
  */
class ApacheAccessLogDecoderSpec {

  @Test
  def parseTest() {
    val rawMessage = """127.0.0.1 - - [05/Feb/2012:17:11:55 +0000] "GET / HTTP/1.1" 200 140 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.5 Safari/535.19""""
    val decoder = ApacheAccessLogDecoder
    val result = decoder.decode(rawMessage.getBytes)
    Assert.assertEquals(Success(ApacheAccessLog("127.0.0.1", "-", "-", "05/Feb/2012:17:11:55 +0000", "GET", "/", "HTTP/1.1", 200, 140)), result)
  }

}
