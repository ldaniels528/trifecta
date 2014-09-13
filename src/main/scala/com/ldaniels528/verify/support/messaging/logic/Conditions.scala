package com.ldaniels528.verify.support.messaging.logic

/**
 * Represents a collection of logical condition operators
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object Conditions {

  /**
   * Represents a logical AND condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class AND(conditionA: Condition, conditionB: Condition) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      conditionA.satisfies(message, key) && conditionB.satisfies(message, key)
    }
  }

  /**
   * The condition is satisfied if any of the conditions evaluate to true
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class ANY(conditions: Condition*) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      conditions.exists(_.satisfies(message, key))
    }
  }

  /**
   * The condition is satisfied if all of the conditions evaluate to true
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class FORALL(conditions: Condition*) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      conditions.forall(_.satisfies(message, key))
    }
  }

  /**
   * Represents a logical OR condition
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  case class OR(conditionA: Condition, conditionB: Condition) extends Condition {
    override def satisfies(message: Array[Byte], key: Option[Array[Byte]]): Boolean = {
      conditionA.satisfies(message, key) || conditionB.satisfies(message, key)
    }
  }

}
