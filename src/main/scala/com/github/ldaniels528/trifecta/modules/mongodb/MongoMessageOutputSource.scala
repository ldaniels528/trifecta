package com.github.ldaniels528.trifecta.modules.mongodb

import com.github.ldaniels528.trifecta.io.json.JsonHelper
import com.github.ldaniels528.trifecta.io.{KeyAndMessage, MessageOutputSource}
import com.github.ldaniels528.trifecta.messages.MessageDecoder
import com.github.ldaniels528.trifecta.modules.ModuleHelper._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * MongoDB Output Source
  * @author lawrence.daniels@gmail.com
  */
class MongoMessageOutputSource(mc: TxMongoCollection) extends MessageOutputSource {

  /**
    * Opens the output source for writing
    */
  override def open() = ()

  /**
    * Writes the given key and decoded message to the underlying stream
    * @param data the given key and message
    */
  override def write(data: KeyAndMessage, decoder: Option[MessageDecoder[_]])(implicit ec: ExecutionContext) = {
    decoder match {
      case Some(aDecoder) => aDecoder.decode(data.message) match {
        case Success(result) => mc.insert(JsonHelper.toJson(result.toString)); ()
        case Failure(e) => die(e.getMessage, e)
      }
      case None => die("No message decoder specified")
    }
  }

  /**
    * Closes the underlying stream
    */
  override def close() = ()

}
