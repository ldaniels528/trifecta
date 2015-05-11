package com.ldaniels528.trifecta.io.mongodb

import com.ldaniels528.commons.helpers.ScalaBeanUtil
import com.mongodb.casbah.Imports._

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
 * Casbah/MongoDB Deserialization Library
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Mongoose extends ScalaBeanUtil {

  /**
   * Creates a new case class (parameterized) instance setting its values
   * from the source database object
   */
  def extract[S](o: DBObject)(implicit m: Manifest[S]): S = {
    // get the destination class
    val destClass = m.runtimeClass.asInstanceOf[Class[S]]

    // extract the bean
    extract(destClass, o)
  }

  /**
   * Creates a new case class instance setting its values
   * from the database object
   */
  def extract[S](destClass: Class[S], o: DBObject): S = {
    // get the source properties
    val srcProps = o.toMap map { case (k, v) => (k.toString, v.asInstanceOf[Object])}

    // lookup the default constructor
    val cons = destClass.getConstructors.head

    // build the destination properties
    val destProps = extractMethods(destClass) map { m =>
      val name = m.getName
      val value = determineValue(m.getReturnType, srcProps.get(name))
      (name, value)
    }

    // create the destination case class
    Try(cons.newInstance(destProps map (_._2): _*).asInstanceOf[S]) match {
      case Success(bean) => bean
      case Failure(e) =>
        throw new IllegalStateException(s"Failed to instance bean class '${destClass.getName}'", e)
    }
  }

  private def determineValue(returnType: Class[_], value: Option[Object]): Object = {
    value match {
      case Some(l: BasicDBList) => getTypedArray(returnType, l)
      case Some(o: DBObject) => extract(returnType, o).asInstanceOf[Object]
      case Some(v) => getTypedValue(returnType, v)
      case None => getDefaultValue(returnType)
    }
  }

  override protected def getTypedArray(returnType: Class[_], value: Object): Array[Any] = {
    // determine the type of the array
    val arrayType = returnType.getComponentType

    value match {
      // create an object array of the items in the DBList
      case l: BasicDBList =>
        val items = (l.listIterator() map {
          case o: DBObject => extract(arrayType, o)
          case x => getTypedValue(arrayType, x)
        }).toArray
        createTypedArray(arrayType, items)

      // allow the super-class to handle anything else
      case _ => super.getTypedArray(returnType, value)
    }
  }

}