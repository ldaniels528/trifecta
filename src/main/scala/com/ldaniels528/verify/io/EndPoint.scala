package com.ldaniels528.verify.io

/**
 * Represents a type-safe remote peer end-point
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait EndPoint extends java.io.Serializable {

  /**
   * Decomposes the end-point into its host and port components
   * @return a tuple containing the host and port
   */
  def apply(): (String, Int) = (host, port)

  /**
   * Returns the host portion of the end-point
   * @return the host portion of the end-point
   */
  def host: String

  /**
   * Returns the port portion of the end-point
   * @return the port portion of the end-point
   */
  def port: Int

  override def toString = if (port != -1) s"$host:$port" else host

}

/**
 * End Point Companion Object
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EndPoint {

  def apply(pair: String, port: Int = -1): EndPoint = {
    pair.split(":").toList match {
      case host :: port :: Nil => new SimpleEndPoint(host, port.toInt)
      case host :: Nil => new SimpleEndPoint(host, port)
      case _ =>
        throw new IllegalStateException(s"Invalid end-point '$pair'; Format 'host:port' expected")
    }
  }

  def unapply(e: EndPoint) = (e.host, e.port)

  /**
   * Simple end-point implementation
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class SimpleEndPoint(host: String, port: Int) extends EndPoint

}