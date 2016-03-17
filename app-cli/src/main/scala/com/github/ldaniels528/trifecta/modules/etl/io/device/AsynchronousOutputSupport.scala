package com.github.ldaniels528.trifecta.modules.etl.io.device

import com.github.ldaniels528.trifecta.modules.etl.io.Scope

import scala.concurrent.{ExecutionContext, Future}

/**
  * Asynchronous Output Support Capability
  * @author lawrence.daniels@gmail.com
  */
trait AsynchronousOutputSupport {
  self: OutputSource =>

  def allWritesCompleted(implicit scope: Scope, ec: ExecutionContext): Future[Seq[OutputSource]]

}
