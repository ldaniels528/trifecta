package com.ldaniels528.trifecta.messages

import com.ldaniels528.trifecta.TxConfig.TxDecoder
import com.ldaniels528.trifecta.io.avro.AvroCodec._
import com.ldaniels528.trifecta.io.avro.AvroMessageDecoding
import com.ldaniels528.trifecta.io.json.JsonTranscoding
import com.ldaniels528.trifecta.messages.logic.Expressions._
import com.ldaniels528.trifecta.messages.logic.{Condition, MessageEvaluation}
import com.ldaniels528.commons.helpers.OptionHelper._
import net.liftweb.json._
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Success, Try}

/**
 * Composite Trifecta Decoder
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class CompositeTxDecoder(decoders: Seq[TxDecoder]) extends AvroMessageDecoding with JsonTranscoding with MessageEvaluation {

  /**
   * Compiles the given operation into a condition
   * @param operation the given operation
   * @return a condition
   */
  override def compile(operation: Expression): Condition = {
    operation match {
      case EQ(field, value) => AvroEQ(this, field, value)
      case GE(field, value) => AvroGE(this, field, value)
      case GT(field, value) => AvroGT(this, field, value)
      case LE(field, value) => AvroLE(this, field, value)
      case LT(field, value) => AvroLT(this, field, value)
      case NE(field, value) => AvroNE(this, field, value)
      case _ => throw new IllegalArgumentException(s"Illegal operation '$operation'")
    }
  }

  /**
   * Decodes the binary message into a typed object
   * @param message the given binary message
   * @return a decoded message wrapped in a Try-monad
   */
  override def decode(message: Array[Byte]): Try[GenericRecord] = {
    val decodedMessage = decoders.foldLeft[Option[GenericRecord]](None) { (result, d) =>
      result ?? attemptDecode(message, d)
    }

    Try(decodedMessage.orDie("Unable to deserialize the message"))
  }

  /**
   * Transcodes the given bytes into JSON
   * @param bytes the given byte array
   * @return a JSON value
   */
  override def toJSON(bytes: Array[Byte]): Try[JValue] = decode(bytes) map (_.toString) map parse

  /**
   * Attempts to decode the given message with the given decoder
   * @param message the given binary message
   * @param txDecoder the given [[TxDecoder]]
   * @return an option of a decoded message
   */
  private def attemptDecode(message: Array[Byte], txDecoder: TxDecoder): Option[GenericRecord] = {
    txDecoder.decoder match {
      case Left(av) =>
        av.decode(message) match {
          case Success(record) => Option(record)
          case Failure(e) => None
        }
      case _ => None
    }
  }

}
