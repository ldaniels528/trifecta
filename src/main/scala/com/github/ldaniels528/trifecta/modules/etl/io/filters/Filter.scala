package com.github.ldaniels528.trifecta.modules.etl.io.filters

import com.github.ldaniels528.trifecta.modules.etl.io.Scope

/**
  * An AngularJS-style filtering mechanism for fields
  * @author lawrence.daniels@gmail.com
  */
trait Filter {

  def execute(value: Option[Any], args: List[String])(implicit scope: Scope): Option[Any]

}
