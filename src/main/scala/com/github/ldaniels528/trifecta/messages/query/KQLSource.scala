package com.github.ldaniels528.trifecta.messages.query

import com.github.ldaniels528.trifecta.io.IOCounter
import com.github.ldaniels528.trifecta.messages.codec.MessageDecoder
import com.github.ldaniels528.trifecta.messages.logic.Condition

import scala.concurrent.{ExecutionContext, Future}

/**
  * KQL Source
  * @author lawrence.daniels@gmail.com
  */
trait KQLSource {

  /**
    * Retrieves messages matching the given criteria up to the given limit.
    * @param fields       the given subset of fields to retrieve
    * @param decoder      the given [[MessageDecoder message decoder]]
    * @param conditions   the given collection of [[Condition conditions]]
    * @param restrictions the given [[KQLRestrictions restrictions]]
    * @param limit        the maximum number of results to return
    * @param counter      the given [[IOCounter I/O counter]]
    * @param ec           the implicit [[ExecutionContext execution context]]
    * @return a promise of a [[KQLResult set of results]]
    */
  def findMany(fields: Seq[String],
               decoder: MessageDecoder[_],
               conditions: Seq[Condition],
               restrictions: KQLRestrictions,
               limit: Option[Int],
               counter: IOCounter)(implicit ec: ExecutionContext): Future[KQLResult]

}
