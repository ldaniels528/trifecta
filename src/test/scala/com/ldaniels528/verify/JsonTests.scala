package com.ldaniels528.verify.util

import org.junit.Test

/**
 * Created by ldaniels on 8/4/14.
 */
object JsonTests {
  import net.liftweb.json._
  implicit val formats = DefaultFormats

  def main(args: Array[String]) {
    val json =
      parse("""{"jmx_port":9999,"timestamp":"1405818758964","host":"vsccrtc204-brn1.rtc.vrsn.com","version":1,"port":9092}""")

    val obj = json.extract[BrokerDetails]
    println(obj)
  }

  case class BrokerDetails(jmx_port: Int, timestamp: String, host: String, version: Int, port: Int)
}


