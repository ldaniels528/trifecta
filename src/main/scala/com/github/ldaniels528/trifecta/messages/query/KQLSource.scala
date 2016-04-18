package com.github.ldaniels528.trifecta.messages.query

import com.github.ldaniels528.trifecta.io.AsyncIO.IOCounter
import com.github.ldaniels528.trifecta.messages.MessageDecoder
import com.github.ldaniels528.trifecta.messages.logic.Condition

import scala.concurrent.{ExecutionContext, Future}

/**
 * KQL Source
 * @author lawrence.daniels@gmail.com
 */
trait KQLSource {

  def findAll(fields: Seq[String],
              decoder: MessageDecoder[_],
              conditions: Seq[Condition],
              limit: Option[Int],
              counter: IOCounter)(implicit ec: ExecutionContext): Future[KQLResult]

}
