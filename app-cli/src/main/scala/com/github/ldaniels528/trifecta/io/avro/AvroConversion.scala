package com.github.ldaniels528.trifecta.io.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.reflect.Method
import java.util.Date

import com.github.ldaniels528.commons.helpers.ResourceHelper._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.slf4j.LoggerFactory

import scala.language.existentials
import scala.util.Try

/**
 * Avro Conversion Utility
 * @author lawrence.daniels@gmail.com
 */
object AvroConversion {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Copies data from a Scala case class to a Java Bean (Avro Builder)
   * @param bean the given Scala case class
   * @param builder the given Java Bean (Avro Builder)
   */
  def copy[A, B](bean: A, builder: B) {
    // retrieve the source data (name-value pairs of the case class)
    val srcData = {
      val beanClass = bean.getClass
      beanClass.getDeclaredFields map (f => (f.getName, beanClass.getMethod(f.getName).invoke(bean), f.getType))
    }

    // populate the Java Bean
    val builderClass = builder.getClass
    srcData foreach { case (name, value, valueClass) =>
      Try {
        val setterName = "set%c%s".format(name.head.toUpper, name.tail)
        setValue(value, valueClass, builder, builderClass, setterName)
      }
    }
  }

  /**
   * Transforms the given JSON string into an Avro encoded-message using the given Avro schema
   * @param json the given JSON string
   * @param schema the given [[Schema]]
   * @return a byte array representing an Avro-encoded message
   */
  def transcodeJsonToAvroBytes(json: String, schema: Schema, encoding: String = "UTF8"): Array[Byte] = {
    new ByteArrayInputStream(json.getBytes(encoding)) use { in =>
      new ByteArrayOutputStream(json.length) use { out =>
        // setup the reader, writer, encoder and decoder
        val reader = new GenericDatumReader[Object](schema)
        val writer = new GenericDatumWriter[Object](schema)

        // transform the JSON into Avro
        val decoder = DecoderFactory.get().jsonDecoder(schema, in)
        val encoder = EncoderFactory.get().binaryEncoder(out, null)
        val datum = reader.read(null, decoder)
        writer.write(datum, encoder)
        encoder.flush()
        out.toByteArray
      }
    }
  }

  /**
   * Transforms the given Avro-encoded binary message into an Avro-specific JSON string
   * @param record the given [[GenericRecord]]
   * @return an Avro-specific JSON string
   */
  def transcodeRecordToAvroJson(record: GenericRecord, encoding: String = "UTF8"): String = {
    transcodeAvroBytesToAvroJson(record.getSchema, encodeRecord(record), encoding)
  }

  /**
   * Transforms the given Avro-encoded binary message into an Avro-specific JSON string
   * @param schema the given [[Schema]]
   * @param bytes the given Avro-encoded binary message
   * @return an Avro-specific JSON string
   */
  def transcodeAvroBytesToAvroJson(schema: Schema, bytes: Array[Byte], encoding: String = "UTF8"): String = {
    new ByteArrayOutputStream(bytes.length * 4) use { out =>
      val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val encoder = EncoderFactory.get().jsonEncoder(schema, out)

      // transform the bytes into Avro-specific JSON
      val reader = new GenericDatumReader[Object](schema)
      val writer = new GenericDatumWriter[Object](schema)
      val datum = reader.read(null, decoder)
      writer.write(datum, encoder)
      encoder.flush()
      out.toString(encoding)
    }
  }

  private def setValue[A, B](value: Any, valueClass: Class[A], dstInst: Any, dstClass: Class[B], setterName: String) {
    val results = value match {
      case o: Option[_] =>
        o.map { myValue =>
          val myObjectValue = myValue.asInstanceOf[Object]
          val myObjectValueClass = Option(myObjectValue) map (_.getClass) getOrElse valueClass
          (myObjectValue, myObjectValueClass)
        }
      case v =>
        val myObjectValue = value.asInstanceOf[Object]
        val myObjectValueClass = Option(myObjectValue) map (_.getClass) getOrElse valueClass
        Option((myObjectValue, myObjectValueClass))
    }

    results foreach { case (myValue, myValueClass) =>
      findMethod(dstClass, setterName, myValueClass) match {
        case Some(m) =>
          m.invoke(dstInst, transformValue(myValue, m.getParameterTypes.head))
        case None =>
          logger.warn(s"value '$value' (${value.getClass.getName}) to [$setterName] - NOT FOUND ['$myValue' (${myValue.getClass.getName})]")
      }
    }
  }

  /**
   * Transforms the given value to the destination type
   * @param value the given value
   * @param dstType the destination type
   * @return the transformed value (or the same value if no transformation was necessary)
   */
  private def transformValue(value: Any, dstType: Class[_]): AnyRef = {
    value match {
      case d: Date if dstType == classOf[java.lang.Long] || dstType == classOf[Long] => d.getTime: java.lang.Long
      case l: Long if dstType == classOf[Date] => new Date(l)
      case l: java.lang.Long if dstType == classOf[Date] => new Date(l)
      case v => v.asInstanceOf[Object]
    }
  }

  private def findMethod(dstClass: Class[_], setterName: String, setterParamClass: Class[_]): Option[Method] = {
    dstClass.getDeclaredMethods find (m =>
      m.getName == setterName &&
        m.getParameterTypes.length == 1 &&
        m.getParameterTypes.headOption.exists(isCompatible(_, setterParamClass)))
  }

  /**
   * Determines compatibility between type A and type B
   * @param typeA the given type A
   * @param typeB the given type B
   * @param first indicates whether this was the first call to the function
   * @return true, if type A and type B are compatible
   */
  private def isCompatible(typeA: Class[_], typeB: Class[_], first: Boolean = true): Boolean = {
    (typeA == typeB) || (typeA match {
      case c if c == classOf[Long] => typeB == classOf[Date]
      case c if c == classOf[java.lang.Long] => typeB == classOf[Date]
      case c if c == classOf[Byte] => typeB == classOf[java.lang.Byte]
      case c if c == classOf[Char] => typeB == classOf[java.lang.Character]
      case c if c == classOf[Double] => typeB == classOf[java.lang.Double]
      case c if c == classOf[Float] => typeB == classOf[java.lang.Float]
      case c if c == classOf[Int] => typeB == classOf[Integer]
      case c if c == classOf[Long] => typeB == classOf[java.lang.Long]
      case c if c == classOf[Short] => typeB == classOf[java.lang.Short]
      case c if first => isCompatible(typeB, typeA, !first)
      case _ => false
    })
  }

  /**
   * Converts the given byte array to an Avro Generic Record
   * @param schema the given Avro Schema
   * @param bytes the given byte array
   * @return an Avro Generic Record
   */
  def decodeRecord(schema: Schema, bytes: Array[Byte]): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }

  /**
   * Converts an Avro Java Bean into a byte array
   * @param datum the given Avro Java Bean
   * @return a byte array
   */
  def encodeRecord[T <: GenericRecord](datum: T): Array[Byte] = {
    new ByteArrayOutputStream(1024) use { out =>
      val writer = new GenericDatumWriter[GenericRecord](datum.getSchema)
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(datum, encoder)
      encoder.flush()
      out.toByteArray
    }
  }

}
