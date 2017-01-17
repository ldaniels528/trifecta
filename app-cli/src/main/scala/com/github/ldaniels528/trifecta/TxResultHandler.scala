package com.github.ldaniels528.trifecta

import scala.concurrent.ExecutionContext

/**
  * Trifecta Result Handler
  * @author lawrence.daniels@gmail.com
  */
trait TxResultHandler {

  /**
    * Handles the processing and/or display of the given result of a command execution
    * @param result the given result
    * @param ec     the given execution context
    */
  def handleResult(result: Any, input: String)(implicit ec: ExecutionContext): Unit

}

/**
  * Trifecta Result Handler Companion
  * @author lawrence.daniels@gmail.com
  */
object TxResultHandler {

  case class Ok() {
    override def toString = "Ok"
  }

}