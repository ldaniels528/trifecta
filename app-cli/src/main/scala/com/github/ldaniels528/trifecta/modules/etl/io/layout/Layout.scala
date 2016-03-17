package com.github.ldaniels528.trifecta.modules.etl.io.layout

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.device.{DataSet, InputSource, OutputSource}
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout.InputSet

/**
  * Represents the logic layout of a text format
  * @author lawrence.daniels@gmail.com
  */
trait Layout {

  def id: String

  def read(device: InputSource)(implicit scope: Scope): Option[InputSet]

  def write(device: OutputSource, inputSet: Option[InputSet])(implicit scope: Scope): Unit

}

/**
  * Layout Companion Object
  * @author lawrence.daniels@gmail.com
  */
object Layout {

  /**
    * Input Set
    */
  case class InputSet(dataSets: Seq[DataSet], offset: Long, isEOF: Boolean)

}