package com.github.ldaniels528.trifecta.modules.etl.io.flow

import com.github.ldaniels528.trifecta.modules.etl.io.OpCode
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSource

import scala.concurrent.Future

/**
  * ETL Process Flow
  * @author lawrence.daniels@gmail.com
  */
trait Flow extends OpCode[Future[Unit]] {

  def id: String

  def devices: Seq[DataSource]

}
