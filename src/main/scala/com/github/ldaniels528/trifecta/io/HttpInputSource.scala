package com.github.ldaniels528.trifecta.io

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.nio.ByteBuffer

/**
 * HTTP Input Source
 * @author lawrence.daniels@gmail.com
 */
class HttpInputSource(url: String) extends InputSource {
  private var conn_? : Option[HttpURLConnection] = None
  private var reader_? : Option[BufferedReader] = None
  private var recordNo: Long = 0L

  /**
   * Reads the given keyed-message from the underlying stream
   * @return the option of a key-and-message
   */
  override def read: Option[KeyAndMessage] = {
    // is the connection open?
    if (conn_?.isEmpty) {
      conn_? = Option(connect)
      reader_? = conn_? map (conn => new BufferedReader(new InputStreamReader(conn.getInputStream)))
      recordNo = 1
    }

    // return the next record
    reader_? map (_.readLine()) map (line => KeyAndMessage(nextKey, line.getBytes))
  }

  /**
   * Closes the underlying stream
   */
  override def close(): Unit = {
    conn_?.foreach(_.disconnect())
    conn_? = None
    reader_? = None
  }

  /**
   * Opens a connection to a remote peer
   * @return the [[HttpURLConnection]]
   */
  private def connect: HttpURLConnection = {
    new URL(url).openConnection().asInstanceOf[HttpURLConnection]
  }

  private def nextKey: Array[Byte] = {
    recordNo += 1
    ByteBuffer.allocate(8).putLong(recordNo).array()
  }

}
