package com.github.ldaniels528.trifecta.messages.codec

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.trifecta.TxConfig.{TxDecoder, TxSuccessSchema}
import com.github.ldaniels528.trifecta.messages.BinaryMessage
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}

import scala.util.Try

/**
  * Composite Message Decoder
  * @author lawrence.daniels@gmail.com
  */
class CompositeMessageDecoder(decoders: Seq[TxDecoder]) extends MessageDecoder[Any]
  with MessageEvaluation {

  /**
    * Compiles the given operation into a condition
    * @param expression the given [[Expression expression]]
    * @return a condition
    */
  override def compile(expression: Expression): Condition = {
    val result = decoders.foldLeft[Option[Condition]](None) { (condition, decoder) =>
      decoder match {
        case me: MessageEvaluation => condition ?? Option(me.compile(expression))
        case _ => condition
      }
    }
    result orDie "No suitable message evaluating decoder found"
  }

  /**
    * Decodes the binary message into a typed object
    * @param message the given binary message
    * @return a decoded message wrapped in a Try-monad
    */
  override def decode(message: Array[Byte]): Try[Any] = {
    val decodedMessage = decoders.foldLeft[Option[Any]](None) { (result, d) =>
      result ?? attemptDecode(message, d)
    }

    Try(decodedMessage orDie "Unable to deserialize the message")
  }

  /**
    * Evaluates the message; returning the resulting field and values
    * @param msg    the given [[BinaryMessage binary message]]
    * @param fields the given subset of fields to return
    * @return the mapping of fields and values
    */
  override def evaluate(msg: BinaryMessage, fields: Seq[String]): Map[String, Any] = {
    val result = decoders.foldLeft[Option[Map[String, Any]]](None) { (mappings, decoder) =>
      decoder match {
        case me: MessageEvaluation => mappings ?? Try(me.evaluate(msg, fields)).toOption
        case _ => mappings
      }
    }
    result getOrElse Map.empty
  }

  /**
    * Attempts to decode the given message with the given decoder
    * @param message   the given binary message
    * @param txDecoder the given [[TxDecoder]]
    * @return an option of a decoded message
    */
  private def attemptDecode(message: Array[Byte], txDecoder: TxDecoder) = {
    txDecoder.decoder match {
      case TxSuccessSchema(_, decoder, _) => decoder.decode(message).toOption
      case _ => None
    }
  }

}
