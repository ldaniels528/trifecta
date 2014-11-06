package com.ldaniels528.trifecta.support.io.query

import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.support.messaging.logic.Condition

import scala.concurrent.{ExecutionContext, Future}

/**
 * Query Source
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait QuerySource {

  def findAll(fields: Seq[String], decoder: MessageDecoder[_], conditions: Seq[Condition], limit: Option[Int])(implicit ec: ExecutionContext): Future[QueryResult]

}
