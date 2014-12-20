package com.ldaniels528.trifecta.messages.query

/**
 * Represents an Input/Output source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class IOSource(deviceURL: String, decoderURL: String) {

  override def toString = s"$deviceURL with $decoderURL"

}
