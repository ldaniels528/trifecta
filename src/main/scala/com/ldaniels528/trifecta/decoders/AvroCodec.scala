package com.ldaniels528.trifecta.decoders

import java.io.{File, FileInputStream, InputStream}
import java.net.URL

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.support.io.Resource
import com.ldaniels528.trifecta.support.io.Resource.expandPath
import com.ldaniels528.trifecta.util.TxUtils._

import scala.io.Source

/**
 * Avro Codec Trait
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroCodec {

  def lookupAvroDecoder(url: String)(implicit config: TxConfig): AvroDecoder = {
    // create an implicit reference to the scope
    implicit val scope = config.scope

    // is it a valid Avro input source?
    val resource_? = url match {
      case s if s.startsWith("classpath:") =>
        for {
          path <- s.extractProperty("classpath:")
          resource <- Resource(path)
        } yield loadAvroDecoder(resource)
      case s if s.startsWith("file:") =>
        s.extractProperty("file:") map expandPath map (path => loadAvroDecoder(new File(path)))
      case s if s.startsWith("http:") =>
        Option(loadAvroDecoder(new URL(s)))
      case s =>
        throw new IllegalStateException(s"Unrecognized Avro URL - $s")
    }

    resource_?.getOrElse(throw new IllegalStateException(s"Malformed Avro URL - $url"))
  }

  def loadAvroDecoder(file: File): AvroDecoder = new FileInputStream(file) use (loadAvroDecoder(file.getName, _))

  def loadAvroDecoder(url: URL): AvroDecoder = url.openStream() use (loadAvroDecoder(url.toURI.toString, _))

  def loadAvroDecoder(label: String, in: InputStream): AvroDecoder = {
    AvroDecoder(label, schemaString = Source.fromInputStream(in).getLines().mkString)
  }

}
