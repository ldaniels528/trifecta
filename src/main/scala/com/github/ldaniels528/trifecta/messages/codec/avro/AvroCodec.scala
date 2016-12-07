package com.github.ldaniels528.trifecta.messages.codec.avro

import java.io.{ByteArrayInputStream, File, InputStream}
import java.net.URL

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.PathHelper._
import com.github.ldaniels528.commons.helpers.Resource
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.util.FileHelper._

import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
  * Avro Codec Singleton
  * @author lawrence.daniels@gmail.com
  */
object AvroCodec {
  private val decoders = TrieMap[String, AvroDecoder]()

  def addDecoder(name: String, schema: String): AvroDecoder = {
    val decoder = loadDecoder(name, new ByteArrayInputStream(schema.getBytes))
    decoders(name) = decoder
    decoder
  }

  def get(name: String): Option[AvroDecoder] = decoders.get(name)

  def loadDecoder(file: File): AvroDecoder = {
    AvroDecoder(file).orDie(s"Suffix ${file.extension.getOrElse("")} is not supported - only .avsc or .avdl are recognized")
  }

  def loadDecoder(url: URL): AvroDecoder = url.openStream() use (loadDecoder(url.toURI.toString, _))

  def loadDecoder(name: String, in: InputStream): AvroDecoder = {
    val decoder = AvroDecoder(name, schemaString = Source.fromInputStream(in).getLines().mkString("\n"))
    decoders(name) = decoder
    decoder
  }

  def resolve(url: String): AvroDecoder = {
    if (!url.contains(":")) decoders.getOrElse(url, throw new IllegalStateException(s"No decoder found for '$url'"))
    else {
      // is it a valid Avro input source?
      val resource_? = url match {
        case s if s.startsWith("classpath:") =>
          for {
            path <- s.extractProperty("classpath:")
            resource <- Resource(path)
          } yield loadDecoder(resource)
        case s if s.startsWith("file:") =>
          s.extractProperty("file:") map expandPath map(s => new File(s)) map (path =>
            AvroDecoder(path).orDie(s"Suffix ${path.extension.getOrElse("")} is not supported - only .avsc or .avdl are recognized"))
        case s if s.startsWith("http:") =>
          Option(loadDecoder(new URL(s)))
        case s =>
          throw new IllegalStateException(s"Unrecognized Avro URL - $s")
      }

      resource_?.getOrElse(throw new IllegalStateException(s"Malformed Avro URL - $url"))
    }
  }

}
