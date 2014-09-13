package com.ldaniels528.verify.support.messaging.logic

/**
 * Operations
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Operations {

  sealed trait Operation

  case class AND(a: Operation, b: Operation) extends Operation

  case class OR(a: Operation, b: Operation) extends Operation

  case class EQ(field: String, value: String) extends Operation

  case class GE(field: String, value: String) extends Operation

  case class GT(field: String, value: String) extends Operation

  case class LE(field: String, value: String) extends Operation

  case class LT(field: String, value: String) extends Operation

  case class NE(field: String, value: String) extends Operation

}
