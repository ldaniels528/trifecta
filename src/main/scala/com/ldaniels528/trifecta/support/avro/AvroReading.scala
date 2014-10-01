package com.ldaniels528.trifecta.support.avro

import java.io.File

import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.util.TxUtils._

import scala.io.Source

/**
 * Avro Reading Trait
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroReading {

  def getAvroDecoder(schemaVar: String)(implicit config: TxConfig): AvroDecoder = {
    // is it an Avro file input source?
    schemaVar.extractProperty("file:") map (loadAvroDecoder(anonymousId, _)) getOrElse {
      // must be a variable reference - get the decoder
      implicit val scope = config.scope
      scope.getVariable(schemaVar).map(_.value).flatMap(_.eval).map(_.asInstanceOf[AvroDecoder])
        .getOrElse(throw new IllegalArgumentException(s"Variable '$schemaVar' not found"))
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

  private def anonymousId: String = s"A${System.currentTimeMillis()}"

}
