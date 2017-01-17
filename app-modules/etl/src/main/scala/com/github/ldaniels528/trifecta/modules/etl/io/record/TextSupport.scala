package com.github.ldaniels528.trifecta.modules.etl.io.record

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSet

/**
  * Represents text-representation support capability for a record
  * @author lawrence.daniels@gmail.com
  */
trait TextSupport {
  self: Record =>

  def fromText(line: String)(implicit scope: Scope): DataSet

  def toText(dataSet: DataSet)(implicit scope: Scope): String

}
