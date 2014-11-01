package com.ldaniels528.trifecta.support.messaging.logic

/**
 * Operations
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Operations {

  sealed trait Operation

  case class AND(a: Operation, b: Operation) extends Operation {
    override def toString = s"$a and $b"
  }

  case class OR(a: Operation, b: Operation) extends Operation {
    override def toString = s"$a or $b"
  }

  case class EQ(field: String, value: String) extends Operation {
    override def toString = s"$field == $value"
  }

  case class GE(field: String, value: String) extends Operation {
    override def toString = s"$field >=$value"
  }

  case class GT(field: String, value: String) extends Operation {
    override def toString = s"$field > $value"
  }

  case class KEY_EQ(value: String) extends Operation {
    override def toString = s"key is $value"
  }

  case class LE(field: String, value: String) extends Operation {
    override def toString = s"$field <= $value"
  }

  case class LT(field: String, value: String) extends Operation {
    override def toString = s"$field < $value"
  }

  case class NE(field: String, value: String) extends Operation {
    override def toString = s"$field != $value"
  }

}
