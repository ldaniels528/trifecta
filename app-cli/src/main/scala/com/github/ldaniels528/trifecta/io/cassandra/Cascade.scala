package com.github.ldaniels528.trifecta.io.cassandra

import java.lang.reflect.{Constructor, Method}

import com.datastax.driver.core._
import com.github.ldaniels528.commons.helpers.ScalaBeanUtil

import scala.collection.JavaConversions._
import scala.collection.concurrent.TrieMap

/**
 * DataStax/Cassandra Object Mapping Utility
 * @author lawrence.daniels@gmail.com
 */
object Cascade extends ScalaBeanUtil {
  // cache of class to constructor and methods
  private val cache = new TrieMap[Class[_], (Constructor[_], Seq[Method])]()

  /**
   * Transforms the contents of the given result set into a
   * collection of instances of the specified bean class.
   */
  def extract[T](beanClass: Class[T], rs: ResultSet): Seq[T] = {
    import scala.language.existentials

    // get the constructor and methods
    val (cons, methods) = getContructorWithMethods(beanClass)

    // construct the bean instances
    val columnDefs = rs.getColumnDefinitions.asList().toSeq

    // construct the bean instances
    rs.all().toSeq map { row =>
      // create a mapping of the data
      val srcProps = decodeRow(row, columnDefs)

      // gather the properties
      val destProps = methods map { m =>
        val name = m.getName.toLowerCase
        val value = srcProps.getOrElse(name, getDefaultValue(m.getReturnType))
        val tvalue = getTypedValue(m.getReturnType, value.asInstanceOf[Object])
        //logger.info(s"$name = '$tvalue' (${if (tvalue != null) tvalue.getClass.getName else "<null>"})")
        tvalue
      }

      // create the destination case class
      cons.newInstance(destProps: _*).asInstanceOf[T]
    }
  }

  private def getContructorWithMethods[T](beanClass: Class[T]): (Constructor[_], Seq[Method]) = {
    cache.get(beanClass) match {
      case Some((cons, methods)) => (cons, methods)
      case None =>
        // lookup the constructor and methods
        val cons = beanClass.getConstructors()(0)
        val methods = extractMethods(beanClass)
        cache += (beanClass ->(cons -> methods))
        (cons, methods)
    }
  }

  private def decodeRow(row: Row, cds: Seq[ColumnDefinitions.Definition]): Map[String, Any] = {
    Map(cds map { cd =>
      val name = cd.getName
      val value = cd.getType.asJavaClass() match {
        case c if c == classOf[Array[Byte]] => row.getBytes(name)
        case c if c == classOf[java.math.BigDecimal] => row.getDecimal(name)
        case c if c == classOf[java.math.BigInteger] => row.getVarint(name)
        case c if c == classOf[java.lang.Boolean] => row.getBool(name)
        case c if c == classOf[java.util.Date] => row.getDate(name)
        case c if c == classOf[java.lang.Double] => row.getDouble(name)
        case c if c == classOf[java.lang.Float] => row.getFloat(name)
        case c if c == classOf[java.lang.Integer] => row.getInt(name)
        case c if c == classOf[java.lang.Long] => row.getLong(name)
        case c if c == classOf[java.util.Map[_, _]] => row.getMap(name, classOf[String], classOf[Object])
        case c if c == classOf[java.util.Set[_]] => row.getSet(name, classOf[Object])
        case c if c == classOf[String] => row.getString(name)
        case c if c == classOf[java.util.UUID] => row.getUUID(name)
        case c =>
          throw new IllegalStateException(s"Unsupported class type ${c.getName} for column ${cd.getTable}.$name")
      }
      (name, value)
    }: _*)
  }

  def mapify[T](bean: T): Map[String, Any] = {
    Map(extractMethods(bean.getClass) map { m =>
      val name = m.getName
      val value = normalizeValue(m.invoke(bean))
      (name, value)
    }: _*)
  }

  private def normalizeValue(value: Any): Any = {
    import org.joda.time.DateTime
    value match {
      case o: Option[_] => normalizeValue(o.orNull)
      case j: DateTime => j.toDate
      case v => v
    }
  }

}