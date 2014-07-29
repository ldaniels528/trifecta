package com.ldaniels528.verify.io

import com.ldaniels528.verify.util.VerifyUtils._
import java.net.URL
import java.net.HttpURLConnection
import java.io.ByteArrayOutputStream
import org.apache.commons.io.IOUtils

/**
 * HTTP Resource
 * @author lawrence.daniels@gmail.com
 */
trait HttpResource {
  import java.io.ByteArrayOutputStream
  import org.apache.commons.io.IOUtils

  def httpGet(urlString: String): Array[Byte] = {
    (new URL(urlString)).openConnection().asInstanceOf[HttpURLConnection] use { conn =>
      conn.setRequestMethod("GET")
      conn.setDoInput(true)

      // get the input
      conn.getInputStream() use { in =>
        val out = new ByteArrayOutputStream(8192)
        IOUtils.copy(in, out)
        out.toByteArray()
      }
    }
  }

  def httpPost(urlString: String, message: String): Array[Byte] = {
    (new URL(urlString)).openConnection().asInstanceOf[HttpURLConnection] use { conn =>
      conn.setRequestMethod("POST")
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setDoOutput(true)

      // write the output
      conn.getOutputStream() use { out =>
        out.write(message.getBytes("UTF8"))
      }
      
      val statusCode = conn.getResponseCode()
      if(statusCode != HttpURLConnection.HTTP_OK)
        throw new IllegalStateException(s"Server returned HTTP/$statusCode")

      // get the input
      conn.getInputStream() use { in =>
        val out = new ByteArrayOutputStream(8192)
        IOUtils.copy(in, out)
        out.toByteArray()
      }
    }
  }

}
