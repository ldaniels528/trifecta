package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.trifecta.sjs.models.Message
import com.github.ldaniels528.trifecta.sjs.services.MessageDataService.{PublishMessageRequest, PublishMessageResponse}
import org.scalajs.angularjs.Service
import org.scalajs.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Message Data Service
  * @author lawrence.daniels@gmail.com
  */
class MessageDataService($http: Http) extends Service {

  def getMessageData(topic: String, partition: Int, offset: Long, decode: Boolean): HttpResponse[Message] = {
    $http.get[Message](s"/api/message/data/${topic.encode}/$partition/$offset?decode=$decode")
  }

  def getMessageKey(topic: String, partition: Int, offset: Long, decode: Boolean): HttpResponse[Message] = {
    $http.get[Message](s"/api/message/key/${topic.encode}/$partition/$offset?decode=$decode")
  }

  def publishMessage(topic: String, key: String, message: String, keyFormat: String, messageFormat: String): HttpResponse[PublishMessageResponse] = {
    $http.post[PublishMessageResponse](
      url = s"/api/message/data/${topic.encode}",
      data = new PublishMessageRequest(key, message, keyFormat, messageFormat))
  }

}

/**
  * Message Data Service Companion
  * @author lawrence.daniels@gmail.com
  */
object MessageDataService {

  /**
    * Publish Message Request
    * @author lawrence.daniels@gmail.com
    */
  @ScalaJSDefined
  class PublishMessageRequest(val key: String, val message: String, val keyFormat: String, val messageFormat: String) extends js.Object

  /**
    * Publish Message Response
    * @author lawrence.daniels@gmail.com
    */
  @ScalaJSDefined
  trait PublishMessageResponse extends js.Object {
    var topic: js.UndefOr[String]
    var offset: js.UndefOr[Long]
    var partition: js.UndefOr[Int]
  }

}
