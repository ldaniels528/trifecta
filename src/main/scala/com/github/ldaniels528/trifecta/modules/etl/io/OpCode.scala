package com.github.ldaniels528.trifecta.modules.etl.io

import scala.concurrent.ExecutionContext

/**
  * Represents an executable opCode
  * @author lawrence.daniels@gmail.com
  */
trait OpCode[T] {

  def execute(scope: Scope)(implicit ec: ExecutionContext): T

}
