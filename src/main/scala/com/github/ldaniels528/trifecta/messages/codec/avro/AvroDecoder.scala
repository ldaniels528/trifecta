package com.github.ldaniels528.trifecta.messages.codec.avro

import java.io.{File, StringReader}

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.codec.json.JsonTransCoding
import com.github.ldaniels528.trifecta.util.FileHelper._
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import net.liftweb.json.JsonAST.JValue
import org.apache.avro.Schema
import org.apache.avro.compiler.idl.Idl
import org.apache.avro.generic.GenericRecord

import scala.util.Try

/**
  * Avro Message Decoder
  * @author lawrence.daniels@gmail.com
  */
case class AvroDecoder(label: String, schema: Schema) extends MessageDecoder[GenericRecord]
  with AvroMessageEvaluation with JsonTransCoding {
  private val converter: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  /**
    * Decodes the binary message (using the Avro schema) into a generic record
    */
  override def decode(message: Array[Byte]): Try[GenericRecord] = converter.invert(message)

  /**
    * Decodes the binary message into a JSON object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decodeAsJson(message: Array[Byte]): Try[JValue] = {
    import net.liftweb.json._
    decode(message) map (record => parse(record.toString))
  }

  override def toString = s"${super.toString}($label)"

}

/**
  * Avro Message Decoder Singleton
  * @author lawrence.daniels@gmail.com
  */
object AvroDecoder {

  def apply(file: File): Option[AvroDecoder] = {
    val name = file.getName
    file.extension.orDie(s"File '$name' has no extension (e.g. '.avsc')") match {
      case ".avsc" =>
        Option(AvroDecoder(name, file.getTextContents))
      case ".avdl" =>
        Option(fromAvDL(name, file.getTextContents))
      case _ => None
    }
  }

  def apply(label: String, schemaString: String): AvroDecoder = {
    val suffix = label.split("\\.").last
    suffix match {
      case "avsc" =>
        new AvroDecoder(label, new Schema.Parser().parse(schemaString))
      case "avdl" =>
        fromAvDL(label, schemaString)
      case _ =>
        throw new IllegalArgumentException(s"Suffix $suffix is not supported - only .avsc or .avdl are recognized")
    }
  }

  private def fromAvDL(label: String, schemaString: String): AvroDecoder = {
    import collection.JavaConverters._

    val name = label.split("\\.").init.mkString.trim
    new StringReader(schemaString) use { rdr =>
      val idlParser = new Idl(rdr)
      val protocol = idlParser.CompilationUnit()
      val schemas = protocol.getTypes.asScala.toSeq
      val mainSchema = schemas.find(_.getName == name) orDie s"File $label does not contain a schema called $name"
      new AvroDecoder(label, mainSchema)
    }
  }

}
