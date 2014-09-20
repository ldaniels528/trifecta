package com.ldaniels528.verify.support.avro

import java.io.ByteArrayOutputStream
import java.lang.reflect.Method

import com.ldaniels528.verify.util.VxUtils._
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.SpecificRecordBase
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Avro Conversion Utility
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object AvroConversion {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  /**
   * Copies data from a Scala case class to a Java Bean (Avro Builder)
   * @param bean the given Scala case class
   * @param builder the given Java Bean (Avro Builder)
   */
  def copy[A, B](bean: A, builder: B) {
    // retrieve the source data (name-value pairs of the case class)
    val srcData = {
      val beanClass = bean.getClass
      beanClass.getDeclaredFields map (f => (f.getName, f.getType, beanClass.getMethod(f.getName).invoke(bean)))
    }

    // populate the Java Bean
    val builderClass = builder.getClass
    srcData foreach { case (name, kind, value) =>
      Try {
        val setter = "set%c%s".format(name.head.toUpper, name.tail)
        builderClass.findMethod(setter, kind) foreach (_.invoke(builder, value: Object))
      }
    }
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
   * @param schema the given Avro Schema
   * @param datum the given Avro Java Bean
   * @return a byte array
   */
  def encodeRecord[T <: SpecificRecordBase](schema: Schema, datum: T): Array[Byte] = {
    new ByteArrayOutputStream(1024) use { out =>
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(datum, encoder)
      encoder.flush()
      out.toByteArray
    }
  }

  /**
   * Adds a convenience method to [[Class]] instances for finding a matching method
   */
  implicit class MethodMagic[T](val beanClass: Class[T]) extends AnyVal {

    def findMethod(name: String, kind: Class[_]): Option[Method] = {
      Try(beanClass.getMethod(name, kind)) match {
        case Success(method) => Option(method)
        case Failure(e) =>
          val method = beanClass.getDeclaredMethods find (m =>
            m.getName == name && m.getParameterTypes.headOption.exists(isCompatible(_, kind)))
          if(method.isEmpty) {
            logger.warn(s"No matching method found for $name with parameter $kind")
          }
          method
      }
    }

    private def isCompatible(typeA: Class[_], typeB: Class[_], first: Boolean = true): Boolean = {
      typeA match {
        case c if c == classOf[Byte] => typeB == classOf[java.lang.Byte]
        case c if c == classOf[Char] => typeB == classOf[java.lang.Character]
        case c if c == classOf[Double] => typeB == classOf[java.lang.Double]
        case c if c == classOf[Float] => typeB == classOf[java.lang.Float]
        case c if c == classOf[Int] => typeB == classOf[Integer]
        case c if c == classOf[Long] => typeB == classOf[java.lang.Long]
        case c if c == classOf[Short] => typeB == classOf[java.lang.Short]
        case c if first => isCompatible(typeB, typeA, !first)
        case _ => false
      }
    }
  }

}
