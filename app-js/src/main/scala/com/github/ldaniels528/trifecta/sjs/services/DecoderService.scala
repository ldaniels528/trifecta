package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.scalascript.Service
import com.github.ldaniels528.scalascript.core.Http
import com.github.ldaniels528.trifecta.sjs.models.{Decoder, DecoderSchema}

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.Dynamic.{global => g, literal}

/**
  * Decoder Service
  * @author lawrence.daniels@gmail.com
  */
class DecoderService($http: Http) extends Service {

  /**
    * Downloads the specified decoder schema
    * @param topic the given topic
    * @param schemaName the given schema name
    */
  def downloadDecoderSchema(topic: String, schemaName: String) = {
    val outcome = $http.get[js.Object](s"/api/schema/${g.encodeURI(topic)}/${g.encodeURI(schemaName)}")
    outcome foreach { data =>
      val blob = js.Dynamic.newInstance(g.Blob)(js.Array(data), literal(`type` = "application/json"))
      val objectUrl = g.URL.createObjectURL(blob)
      g.window.open(objectUrl)
    }
    outcome
  }

  /**
    * Retrieves the decoder associated to the specified topic
    * @param topic the specified topic
    * @return the requested [[Decoder decoder]]
    */
  def getDecoderByTopic(topic: String) = {
    $http.get[Decoder](s"/api/decoders/topic/${g.encodeURI(topic)}")
  }

  /**
    * Retrieves all decoders (regardless of topic)
    * @return an array of [[Decoder decoders]]
    */
  def getDecoders = {
    $http.get[js.Array[Decoder]]("/api/decoders")
  }

  /**
    * Retrieves the specified schema for the given topic
    * @param topic the specified topic
    * @param schemaName the specified schema
    * @return the decoder schema
    */
  def getDecoderSchema(topic: String, schemaName: String) = {
    $http.get[DecoderSchema](s"/api/schema/${g.encodeURI(topic)}/${g.encodeURI(schemaName)}")
  }

  /**
    * Saves the schema to the server
    * @param schema the given schema
    * @return the response code
    */
  def saveDecoderSchema(schema: DecoderSchema) = {
    $http.post[DecoderSchema](
      url = "/api/schema",
      headers = js.Dictionary("Content-Type" -> "application/json"),
      data = literal(
        topic = schema.topic,
        name = schema.name,
        schemaString = schema.schemaString
      ))
  }

}
