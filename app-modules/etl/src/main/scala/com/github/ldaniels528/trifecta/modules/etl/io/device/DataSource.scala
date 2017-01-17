package com.github.ldaniels528.trifecta.modules.etl.io.device

import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout

/**
  * Represents a Generic Input or Output Source
  * @author lawrence.daniels@gmail.com
  */
trait DataSource extends StatisticsGeneration {

  def id: String

  def close(implicit scope: Scope): Unit

  def layout: Layout

  def open(implicit scope: Scope): Unit

}

/**
  * Data Source Companion Object
  * @author lawrence.daniels@gmail.com
  */
object DataSource {

  implicit class DataSourceEnrichment[T <: DataSource](val source: T) extends AnyVal {

    def use[S](block: T => S)(implicit scope: Scope): S = {
      source.open(scope)
      try block(source) finally source.close(scope)
    }

  }

}
