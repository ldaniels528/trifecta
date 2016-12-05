package com.github.ldaniels528.trifecta.messages.logic

import com.github.ldaniels528.trifecta.messages.BinaryMessage
import com.github.ldaniels528.trifecta.messages.codec.avro.JsonTransCoding
import com.github.ldaniels528.trifecta.messages.logic.Expressions._
import com.github.ldaniels528.trifecta.messages.logic.JsonMessageEvaluation._
import com.github.ldaniels528.trifecta.messages.logic.MessageEvaluation._
import net.liftweb.json._
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * JSON Message Evaluation
  * @author lawrence.daniels@gmail.com
  */
trait JsonMessageEvaluation extends MessageEvaluation {
  self: JsonTransCoding =>

  /**
    * Compiles the given operation into a condition
    * @param operation the given operation
    * @return a condition
    */
  override def compile(operation: Expression): Condition = {
    operation match {
      case EQ(field, value) => JsonEQ(this, field, value)
      case GE(field, value) => JsonGE(this, field, value)
      case GT(field, value) => JsonGT(this, field, value)
      case LE(field, value) => JsonLE(this, field, value)
      case LT(field, value) => JsonLT(this, field, value)
      case NE(field, value) => JsonNE(this, field, value)
      case _ => throw new IllegalArgumentException(s"Illegal operation '$operation'")
    }
  }

  /**
    * Evaluates the message; returning the resulting field and values
    * @param msg    the given [[BinaryMessage binary message]]
    * @param fields the given subset of fields to return
    * @return the mapping of fields and values
    */
  override def evaluate(msg: BinaryMessage, fields: Seq[String]): Map[String, Any] = {
    decodeAsJson(msg.message) match {
      case Success(JObject(mapping)) =>
        Map(mapping.map(f => f.name -> unwrap(f.value)): _*) filter {
          case (k, v) => fields.isAllFields || fields.contains(k)
        }
      case Success(_) => Map.empty
      case Failure(e) =>
        throw new IllegalStateException("Malformed JSON message", e)
    }
  }

}

/**
  * JSON Message Evaluation Companion
  * @author lawrence.daniels@gmail.com
  */
object JsonMessageEvaluation {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  def unwrap(jv: JValue): Any = {
    jv match {
      case JArray(values) => values map unwrap
      case JBool(value) => value
      case JDouble(num) => num
      case JInt(num) => num
      case JObject(fields) => Map(fields.map(f => f.name -> unwrap(f.value)): _*)
      case JNull => null
      case JString(s) => s
      case unknown =>
        logger.warn(s"Unrecognized typed '$unknown' (${unknown.getClass.getName})")
        null
    }
  }

  /**
    * Json Field-Value Equality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonEQ(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => value == null
            case JBool(b) => Try(value.toBoolean).toOption.contains(b)
            case JDouble(n) => Try(value.toDouble).toOption.contains(n)
            case JInt(n) => Try(value.toDouble).toOption.contains(n.toDouble)
            case JString(s) => s == value
            case x =>
              logger.warn(s"Value '$x' (${Option(x).map(_.getClass.getName).orNull}) for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field == '$value'"
  }

  /**
    * Json Field-Value Greater-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonGT(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => false
            case JBool(b) => false
            case JDouble(n) => Try(value.toDouble).toOption.exists(n > _)
            case JInt(n) => Try(value.toDouble).toOption.exists(n.toDouble > _)
            case JString(s) => s > value
            case x =>
              logger.warn(s"Value '$x' for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field > $value'"
  }

  /**
    * Json Field-Value Greater-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonGE(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => false
            case JBool(b) => false
            case JDouble(n) => Try(value.toDouble).toOption.exists(n >= _)
            case JInt(n) => Try(value.toDouble).toOption.exists(n.toDouble >= _)
            case JString(s) => s >= value
            case x =>
              logger.warn(s"Value '$x' for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field >= '$value'"
  }

  /**
    * Json Field-Value Less-Than Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonLT(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => false
            case JBool(b) => false
            case JDouble(n) => Try(value.toDouble).toOption.exists(n < _)
            case JInt(n) => Try(value.toDouble).toOption.exists(n.toDouble < _)
            case JString(s) => s < value
            case x =>
              logger.warn(s"Value '$x' for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field < '$value'"
  }

  /**
    * Json Field-Value Less-Than-Or-Equal Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonLE(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => false
            case JBool(b) => false
            case JDouble(n) => Try(value.toDouble).toOption.exists(n <= _)
            case JInt(n) => Try(value.toDouble).toOption.exists(n.toDouble <= _)
            case JString(s) => s <= value
            case x =>
              logger.warn(s"Value '$x' for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field <= '$value'"
  }

  /**
    * Json Field-Value Inequality Condition
    * @author lawrence.daniels@gmail.com
    */
  case class JsonNE(decoder: JsonTransCoding, field: String, value: String) extends Condition {
    override def satisfies(message: Array[Byte], key: Array[Byte]): Boolean = {
      decoder.decodeAsJson(message) match {
        case Success(js) =>
          js \ field match {
            case JNull => value != null
            case JBool(b) => !Try(value.toBoolean).toOption.contains(b)
            case JDouble(n) => !Try(value.toDouble).toOption.contains(n)
            case JInt(n) => !Try(value.toDouble).toOption.contains(n.toDouble)
            case JString(s) => s != value
            case x =>
              logger.warn(s"Value '$x' for field '$field' was not recognized")
              false
          }
        case Failure(e) => false
      }
    }

    override def toString = s"$field != '$value'"
  }

}