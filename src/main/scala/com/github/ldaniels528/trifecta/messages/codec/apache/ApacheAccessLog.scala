package com.github.ldaniels528.trifecta.messages.codec.apache

import java.util.regex.Pattern

/**
  * Represents an Apache Access Log message
  * @author lawrence.daniels@gmail.com
  */
case class ApacheAccessLog(ipAddress: String,
                           clientIdentd: String,
                           userID: String,
                           dateTimeString: String,
                           method: String,
                           endpoint: String,
                           protocol: String,
                           responseCode: Int,
                           contentSize: Long) {

  override def toString: String = {
    """%s %s %s [%s] "%s %s %s" %d %d""".format(
      ipAddress, clientIdentd, userID, dateTimeString, method, endpoint,
      protocol, responseCode, contentSize)
  }
}

/**
  * Apache Access Log Companion
  * @author lawrence.daniels@gmail.com
  */
object ApacheAccessLog {
  private val LOG_ENTRY_PATTERN =
  //  1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
    "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"

  private val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)

  def apply(logLine: String): ApacheAccessLog = {
    val m = PATTERN.matcher(logLine)
    if (!m.find()) {
      throw new RuntimeException("Error parsing log line")
    }

    ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
      m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
  }

}
