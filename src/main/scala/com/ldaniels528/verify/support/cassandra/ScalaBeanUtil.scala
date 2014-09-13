package com.ldaniels528.verify.support.cassandra

import scala.util.{ Try, Success, Failure }
import java.lang.reflect.{ Field, Method }
import scala.collection.JavaConverters._

/**
 * Scala Bean Copy Utilities
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ScalaBeanUtil() {

  /**
   * Copies values from the source instance to the destination instance
   * via the destination's mutator methods.
   */
  def copy[A, B](src: A, dest: B): B = {
    // get the source and destination fields
    val srcMethods = extractMethods(src.getClass)
    val destMethods = extractMethods(dest.getClass, isTarget = true)

    // for each field that's found in both source and destination,
    // copy its value
    destMethods foreach {
      dm =>
        // find the matching method
        srcMethods.find(_.getName == asGetter(dm)) match {
          case Some(sm) =>
            val value = getValue(src, sm)
            Try(setValue(dest, dm, value)) match {
              case Success(_) =>
              case Failure(e) =>
                System.err.println(s"ScalaBeanUtil: Error setting method '${sm.getName}' with '$value'")
            }
          case None =>
        }
    }
    dest
  }

  /**
   * Creates a new case class (parameterized) instance setting its values
   * from the source object(s)
   */
  def caseCopy[S](sources: Any*)(implicit m: Manifest[S]): S = {
    // get the source properties
    val srcProps = Map(sources flatMap (src =>
      extractMethods(src.getClass) flatMap { m =>
        val (k, v) = (m.getName, m.invoke(src))
        if (v != getDefaultValue(m.getReturnType)) Some((k, v)) else None
      }): _*)

    // get the destination class
    val destClass = m.erasure.asInstanceOf[Class[S]]

    // lookup the default constructor
    val cons = destClass.getConstructors()(0)

    // if the construct has no parameters, set the mutators
    if (cons.getParameterTypes.length == 0) {
      // create the destination instance
      val dest = cons.newInstance().asInstanceOf[S]

      // copy the values
      extractMethods(destClass, isTarget = true) map (m =>
        setValue(dest, m, srcProps.getOrElse(asGetter(m), getDefaultValue(m.getReturnType))))
      dest
    } else {
      // build the destination properties
      val destProps = extractMethods(destClass) map { m =>
        val name = m.getName
        (name, srcProps.getOrElse(name, getDefaultValue(m.getReturnType)))
      }

      // create the destination case class
      cons.newInstance(destProps map (_._2): _*).asInstanceOf[S]
    }
  }

  /**
   * Retrieves the value from the bean's property
   */
  protected def getValue[A](bean: A, m: Method): Object = m.invoke(bean)

  /**
   * Sets the value of the bean's property
   */
  protected def setValue[B](bean: B, m: Method, value: Object): Object = {
    m.invoke(bean, getTypedValue(m.getReturnType, value))
  }

  /**
   * Returns the default value of the method's return type
   */
  protected def getDefaultValue(returnType: Class[_]): Object = {
    returnType match {
      case c if c.isArray => createTypedArray(returnType.getComponentType, Array.empty)
      case c if c == classOf[Option[_]] => None
      case c if c == classOf[List[_]] => Nil
      case c if c == classOf[Seq[_]] => Seq.empty
      case c if c == classOf[Set[_]] => Set.empty
      case c if c == classOf[java.util.Collection[_]] => java.util.Collections.emptyList
      case c if c == classOf[java.util.List[_]] => java.util.Collections.emptyList
      case c if c == classOf[java.util.Set[_]] => java.util.Collections.emptySet
      case _ => null
    }
  }

  protected def getTypedValue(returnType: Class[_], value: Object): Object = {
    import scala.collection.JavaConversions._

    // look for exception cases by value type
    value match {
      // if the value is null ...
      case v if v == null => getDefaultValue(returnType)

      // if the value is an Option[T] ...
      case o: Option[_] if returnType != classOf[Option[_]] => (o getOrElse null).asInstanceOf[Object]

      // look for exception cases by return type
      case _ =>
        returnType match {
          case c if c.isArray => getTypedArray(returnType, value)
          case c if c == classOf[Option[_]] => Option(value)
          case c if c == classOf[List[_]] => getTypedArray(returnType, value).toList
          case c if c == classOf[Seq[_]] => getTypedArray(returnType, value).toSeq
          case c if c == classOf[Set[_]] => getTypedArray(returnType, value).toSet
          case c if c == classOf[java.util.Collection[_]] => seqAsJavaList(getTypedArray(returnType, value).toList)
          case c if c == classOf[java.util.List[_]] => seqAsJavaList(getTypedArray(returnType, value).toList)
          case c if c == classOf[java.util.Set[_]] => getTypedArray(returnType, value).toSet.asJava
          case _ => value
        }
    }
  }

  protected def getTypedArray(returnType: Class[_], value: Object): Array[Any] = {
    // determine the type of the array
    val arrayType = returnType.getComponentType

    // convert the value into an array
    value match {
      case a if a == null => createTypedArray(arrayType, Array.empty)
      case a: Array[_] => createTypedArray(arrayType, a.toArray)
      case l: List[_] => createTypedArray(arrayType, l.toArray)
      case s: Seq[_] => createTypedArray(arrayType, s.toArray)
      case c: java.util.Collection[_] => createTypedArray(arrayType, c.toArray.asInstanceOf[Array[Any]])
      case x =>
        throw new IllegalArgumentException(s"Cannot convert type ${x.getClass.getName} into an array")
    }
  }

  protected def createTypedArray(arrayType: Class[_], items: Array[Any]): Array[Any] = {
    val array = java.lang.reflect.Array.newInstance(arrayType, items.length).asInstanceOf[Array[Any]]
    System.arraycopy(items, 0, array, 0, items.length)
    array
  }

  protected def asGetter(m: Method): String = m.getName.replaceAllLiterally("_$eq", "")

  protected def extractMethods[A](beanClass: Class[A], isTarget: Boolean = false): Seq[Method] = {
    if (isTarget) {
      beanClass.getDeclaredMethods filter (_.getName.endsWith("_$eq"))
    } else {
      beanClass.getDeclaredFields filterNot unwantedFields map (f => beanClass.getMethod(f.getName))
    }
  }

  /**
   * Eliminates reflection artifacts
   */
  protected def unwantedFields(f: Field) = Set("$outer", "serialVersionUID").contains(f.getName)

}