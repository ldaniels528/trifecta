package com.ldaniels528.verify.codecs

import com.ldaniels528.verify.support.messaging.logic.Condition

/**
 * Message Comparator
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait MessageComparator {

  def equals(field: String, value: String): Condition

  def notEqual(field: String, value: String): Condition

  def greaterThan(field: String, value: String): Condition

  def greaterOrEqual(field: String, value: String): Condition

  def lessThan(field: String, value: String): Condition

  def lessOrEqual(field: String, value: String): Condition

}
