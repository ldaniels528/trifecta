package com.ldaniels528.trifecta.command.parser.bdql

import com.ldaniels528.trifecta.{TxConfig, TxRuntimeContext}
import com.ldaniels528.trifecta.decoders.MessageCodecs
import com.ldaniels528.trifecta.support.messaging.MessageDecoder
import com.ldaniels528.trifecta.support.messaging.logic.ConditionCompiler._
import com.ldaniels528.trifecta.support.messaging.logic.Expressions.Expression

import scala.concurrent.ExecutionContext

/**
 * Big Data Selection Query
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class BigDataSelection(source: IOSource,
                            destination: Option[IOSource] = None,
                            fields: Seq[String],
                            criteria: Option[Expression],
                            limit: Option[Int]) {

  /**
   * Executes the selection query
   * @param rt the given runtime context
   */
  def execute(implicit config: TxConfig, rt: TxRuntimeContext, ec: ExecutionContext) = {
    // get the input source and its decoder
    val inputSource = rt.getInputHandler(source.deviceURL)
    val decoder: Option[MessageDecoder[_]] = MessageCodecs.getDecoder(source.decoderURL)

    // get the output source and its encoder
    val outputSource = destination.flatMap(src => rt.getOutputHandler(src.deviceURL))
    val encoder: Option[MessageDecoder[_]] = destination.flatMap(src => MessageCodecs.getDecoder(src.decoderURL))

    // get all other properties
    val conditions = criteria.map(compile(_, decoder))
    val count = limit.getOrElse(10)

    // perform the query/copy operation
    inputSource.foreach { in =>
      for (n <- 1 to count) {
        for {
          block <- in.read
          condition <- conditions
          out <- outputSource if condition.satisfies(block.message, block.key)
        } {
          out.write(block, encoder)
        }
      }
    }
  }

  override def toString = {
    val sb = new StringBuilder(s"select ${fields.mkString(", ")} from $source")
    destination.foreach(dest => sb.append(s" into $dest"))
    if (criteria.nonEmpty) {
      sb.append(" where ")
      sb.append(criteria.map(_.toString) mkString " ")
    }
    limit.foreach(count => sb.append(s" limit $count"))
    sb.toString()
  }

}

case class IOSource(deviceURL: String, decoderURL: String) {
  override def toString = s"$deviceURL with $decoderURL"
}