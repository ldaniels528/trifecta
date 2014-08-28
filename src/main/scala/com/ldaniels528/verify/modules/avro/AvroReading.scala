package com.ldaniels528.verify.modules.avro

import java.io.File

import com.ldaniels528.verify.VxRuntimeContext

import scala.io.Source

/**
 * Avro Reading Trait
 */
trait AvroReading {

  def getAvroDecoder(schemaVar: String)(implicit rt: VxRuntimeContext): AvroDecoder = {
    // get the decoder
    implicit val scope = rt.scope
    scope.getVariable(schemaVar).map(_.value).flatMap(_.eval).map(_.asInstanceOf[AvroDecoder])
      .getOrElse(throw new IllegalArgumentException(s"Variable '$schemaVar' not found"))
  }

   def loadAvroDecoder(schemaPath: String): AvroDecoder = {
    // ensure the file exists
    val schemaFile = new File(schemaPath)
    if (!schemaFile.exists()) {
      throw new IllegalStateException(s"Schema file '${schemaFile.getAbsolutePath}' not found")
    }

    // retrieve the schema as a string
    val schemaString = Source.fromFile(schemaFile).getLines() mkString "\n"
    new AvroDecoder(schemaString)
  }

}
