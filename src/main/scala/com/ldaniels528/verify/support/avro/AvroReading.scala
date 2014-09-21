package com.ldaniels528.verify.support.avro

import java.io.File

import com.ldaniels528.verify.VxRuntimeContext

import scala.io.Source

/**
 * Avro Reading Trait
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroReading {

  def getAvroDecoder(schemaVar: String)(implicit rt: VxRuntimeContext): AvroDecoder = {
    // get the decoder
    implicit val scope = rt.config.scope
    scope.getVariable(schemaVar).map(_.value).flatMap(_.eval).map(_.asInstanceOf[AvroDecoder])
      .getOrElse(throw new IllegalArgumentException(s"Variable '$schemaVar' not found"))
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
