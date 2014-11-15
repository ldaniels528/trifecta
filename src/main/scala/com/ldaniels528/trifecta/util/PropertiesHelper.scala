package com.ldaniels528.trifecta.util

import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Properties Helper Utility Class
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object PropertiesHelper {

  /**
   * Properties Conversion
   */
  implicit class PropertiesConversion[T <: Object](val m: Map[String, T]) extends AnyVal {

    import java.util.Properties

    def toProps: Properties = {
      val p = new Properties()
      p.putAll(m.asJava)
      p
    }
  }

  /**
   * Syntactic Sugar for Properties object
   */
  implicit class PropertiesExtensions(val props: Properties) extends AnyVal {

    def asOpt[T](key: String): Option[T] = {
      Option(props.get(key)) map (_.asInstanceOf[T])
    }

    def getOrElse(key: String, default: => String): String = {
      Option(props.getProperty(key)).getOrElse(default)
    }

  }

}
