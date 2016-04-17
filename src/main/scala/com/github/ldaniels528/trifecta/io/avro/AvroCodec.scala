package com.github.ldaniels528.trifecta.io.avro

import java.io.{ByteArrayInputStream, File, FileInputStream, InputStream}
import java.net.URL

import com.github.ldaniels528.trifecta.messages.MessageDecoder
import com.github.ldaniels528.trifecta.messages.logic.Condition
import com.github.ldaniels528.commons.helpers.PathHelper._
import com.github.ldaniels528.commons.helpers.Resource
import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8

import scala.collection.concurrent.TrieMap
import scala.io.Source
import scala.util.{Failure, Success}

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

  def loadDecoder(file: File): AvroDecoder = new FileInputStream(file) use (loadDecoder(file.getName, _))

  def loadDecoder(url: URL): AvroDecoder = url.openStream() use (loadDecoder(url.toURI.toString, _))

  def loadDecoder(name: String, in: InputStream): AvroDecoder = {
    val decoder = AvroDecoder(name, schemaString = Source.fromInputStream(in).getLines().mkString)
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
          s.extractProperty("file:") map expandPath map (path => loadDecoder(new File(path)))
        case s if s.startsWith("http:") =>
          Option(loadDecoder(new URL(s)))
        case s =>
          throw new IllegalStateException(s"Unrecognized Avro URL - $s")
      }

      resource_?.getOrElse(throw new IllegalStateException(s"Malformed Avro URL - $url"))
    }
  }

  /**
    * Avro Field-Value Equality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroEQ(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v if v == null => value == null
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() == value.toDouble
            case s: String => s == value
            case x =>
              throw new IllegalStateException(s"Value '$x' (${Option(x).map(_.getClass.getName).orNull}) for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field == '$value'"
  }

  /**
    * Avro Field-Value Greater-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroGT(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() > value.toDouble
            case s: String => s > value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field > $value'"
  }

  /**
    * Avro Field-Value Greater-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroGE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() >= value.toDouble
            case s: String => s >= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field >= '$value'"
  }

  /**
    * Avro Field-Value Less-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroLT(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() < value.toDouble
            case s: String => s < value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field < '$value'"
  }

  /**
    * Avro Field-Value Less-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroLE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case null => false
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() <= value.toDouble
            case s: String => s <= value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field <= '$value'"
  }

  /**
    * Avro Field-Value Inequality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class AvroNE(decoder: MessageDecoder[GenericRecord], field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decode(message) match {
        case Success(record) =>
          record.get(field) match {
            case v if v == null => value != null
            case v: Utf8 => v.toString == value
            case v: java.lang.Number => v.doubleValue() != value.toDouble
            case s: String => s != value
            case x => throw new IllegalStateException(s"Value '$x' for field '$field' was not recognized")
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field != '$value'"
  }

}