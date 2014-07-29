package com.ldaniels528.verify.io

/**
 * Represents a type-safe remote peer end-point
 * @author lawrence.daniels@gmail.com
 */
class EndPoint(val host: String, val port: Int) extends java.io.Serializable {

  override def toString = if (port != -1) s"$host:$port" else host

}

/**
 * End Point Singleton
 * @author lawrence.daniels@gmail.com
 */
object EndPoint {

  def apply(pair: String, port: Int = -1): EndPoint = {
    pair.split(":").toList match {
      case host :: port :: Nil => new EndPoint(host, port.toInt)
      case host :: Nil => new EndPoint(host, port)
      case _ =>
        throw new IllegalStateException(s"Invalid end-point '$pair'; Format 'host:port' expected")
    }
  }

  def unapply(e: EndPoint) = (e.host, e.port)

}