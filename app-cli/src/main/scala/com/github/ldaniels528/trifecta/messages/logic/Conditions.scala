package com.github.ldaniels528.trifecta.messages.logic

import com.github.ldaniels528.trifecta.messages.BinaryMessaging

/**
 * Represents a collection of logical condition operators
 * @author lawrence.daniels@gmail.com
 */
object Conditions extends BinaryMessaging {

  /**
   * Represents a key equality condition
   * @author lawrence.daniels@gmail.com
   */
  case class KeyIs(myKey: Array[Byte]) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]) = myKey sameElements key
  }

  /**
   * Represents a logical AND condition
   * @author lawrence.daniels@gmail.com
   */
  case class AND(conditionA: Condition, conditionB: Condition) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      conditionA.satisfies(message, key) && conditionB.satisfies(message, key)
    }
  }

  /**
   * The condition is satisfied if any of the conditions evaluate to true
   * @author lawrence.daniels@gmail.com
   */
  case class ANY(conditions: Condition*) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      conditions.exists(_.satisfies(message, key))
    }
  }

  /**
   * The condition is satisfied if all of the conditions evaluate to true
   * @author lawrence.daniels@gmail.com
   */
  case class FORALL(conditions: Condition*) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      conditions.forall(_.satisfies(message, key))
    }
  }

  /**
   * Represents a logical OR condition
   * @author lawrence.daniels@gmail.com
   */
  case class OR(conditionA: Condition, conditionB: Condition) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      conditionA.satisfies(message, key) || conditionB.satisfies(message, key)
    }
  }

}
