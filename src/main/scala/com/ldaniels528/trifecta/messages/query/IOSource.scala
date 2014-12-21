package com.ldaniels528.trifecta.messages.query

/**
 * Represents an Input/Output source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class IOSource(deviceURL: String, decoderURL: Option[String]) {

  override def toString = s"$deviceURL${decoderURL.map(url => s" with $url" ) getOrElse ""}"

}
