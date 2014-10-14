package com.ldaniels528.trifecta.modules

import java.io.File

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.decoders.AvroDecoder
import com.ldaniels528.trifecta.util.TxUtils._

import scala.io.Source

/**
 * Avro Reading Trait
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroReading {

  def lookupAvroDecoder(url: String)(implicit config: TxConfig): AvroDecoder = {
    // create an implicit reference to the scope
    implicit val scope = config.scope

    // is it a valid Avro input source?
    url match {
      case s if s.startsWith("file:") =>
        (s.extractProperty("file:") map (path => loadAvroDecoder(new File(path).getName, path)))
          .getOrElse(throw new IllegalStateException(s"Unrecognized Avro URL - $s"))
      case s if s.startsWith("http:") =>
        throw new IllegalStateException("HTTP URLs are not yet supported")
      case s =>
        throw new IllegalStateException(s"Unrecognized Avro URL - $s")
    }
  }

  def loadAvroDecoder(label: String, schemaPath: String): AvroDecoder = {
    // ensure the file exists
    val schemaFile = new File(schemaPath)
    if (!schemaFile.exists()) {
      throw new IllegalStateException(s"Schema file '${schemaFile.getAbsolutePath}' not found")
    }

    // retrieve the schema as a string
    val schemaString = Source.fromFile(schemaFile).getLines() mkString "\n"
    AvroDecoder(label, schemaString)
  }

}
