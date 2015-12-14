package com.github.ldaniels528.trifecta.models

import play.api.libs.json.{JsValue, Json}

/**
  * Formatted Data JSON model
  * @author lawrence.daniels@gmail.com
  */
case class FormattedDataJs(`type`: String, value: JsValue)

/**
  * Formatted Data JSON Companion Object
  * @author lawrence.daniels@gmail.com
  */
object FormattedDataJs {

  implicit val FormattedDataReads = Json.reads[FormattedDataJs]

  implicit val FormattedDataWrites = Json.writes[FormattedDataJs]

}
