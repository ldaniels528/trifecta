package com.kbb.trifecta.decoders

import com.github.ldaniels528.trifecta.messages.codec.json.JsonHelper
import net.liftweb.json.JValue

/**
  * KBB Log Raw Message
  * @author lawrence.daniels@gmail.com
  */
case class LogRawMessage(epoc: Long,
                         timestamp: String,
                         format: String,
                         ipAddress: String,
                         request: JValue,
                         log: String) {

  override def toString: String = {
    Seq(epoc, timestamp, format, ipAddress, JsonHelper.compressJson(request), log) mkString "\t"
  }

}

/**
  * KBB Log Raw Message Companion
  * @author lawrence.daniels@gmail.com
  */
object LogRawMessage {

  /**
    * Parses the given message into a new LogRaw message instance
    * @param message the given binary message
    * @return a new [[LogRawMessage LogRaw message]] instance
    */
  def apply(message: Array[Byte]): LogRawMessage = {
    LogRawMessage(new String(message))
  }

  /**
    * Parses the given message into a new LogRaw message instance
    * @param message the given textual message
    * @return a new [[LogRawMessage LogRaw message]] instance
    */
  def apply(message: String): LogRawMessage = {
    message.split("[\t]", 6).toList match {
      case epoc :: ts :: format :: ip :: request :: log :: Nil =>
        LogRawMessage(epoc.toLong, ts, format, ip, JsonHelper.toJson(request), log)
      case _ =>
        throw new IllegalArgumentException("Invalid Log-raw message")
    }
  }

}