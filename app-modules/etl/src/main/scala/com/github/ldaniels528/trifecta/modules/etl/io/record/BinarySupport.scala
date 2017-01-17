package com.github.ldaniels528.trifecta.modules.etl.io.record

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet

/**
  * Represents binary-representation support capability for a record
  * @author lawrence.daniels@gmail.com
  */
trait BinarySupport {
  self: Record =>

  def fromBytes(bytes: Array[Byte])(implicit scope: Scope): DataSet

  def toBytes(dataSet: DataSet)(implicit scope: Scope): Array[Byte]

}
