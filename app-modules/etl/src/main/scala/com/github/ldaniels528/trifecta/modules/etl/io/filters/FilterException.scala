package com.github.ldaniels528.trifecta.modules.etl.io.filters

/**
  * Filter Exception
  */
class FilterException(filter: Filter, message: String)
  extends RuntimeException(s"${filter.getClass.getSimpleName}: $message")