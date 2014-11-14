package com.ldaniels528.trifecta.messages.query

import com.ldaniels528.trifecta.messages.MessageDecoder
import com.ldaniels528.trifecta.messages.logic.Condition

import scala.concurrent.{ExecutionContext, Future}

/**
 * Query Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait QuerySource {

  def findAll(fields: Seq[String], decoder: MessageDecoder[_], conditions: Seq[Condition], limit: Option[Int])(implicit ec: ExecutionContext): Future[QueryResult]

}
