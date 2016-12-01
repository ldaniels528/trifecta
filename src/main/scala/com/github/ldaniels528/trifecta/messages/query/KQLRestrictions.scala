package com.github.ldaniels528.trifecta.messages.query

/**
  * Represents a collection of restrictions; e.g. offset delta
  * @author lawrence.daniels@gmail.com
  */
case class KQLRestrictions(delta: Option[Long] = None, groupId: Option[String] = None) {

  override def toString: String = {
    var list: List[String] = Nil
    delta.foreach(delta => list = s"using delta $delta" :: list)
    groupId.foreach(group => list = s"using consumer $group" :: list)
    list.mkString(" ")
  }

}