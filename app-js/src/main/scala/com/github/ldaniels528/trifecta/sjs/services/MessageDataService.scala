package com.github.ldaniels528.trifecta.sjs.services

import org.scalajs.angularjs.Service
import org.scalajs.angularjs.http.Http
import org.scalajs.dom.browser.encodeURI
import com.github.ldaniels528.trifecta.sjs.models.{Message, PublishMessageResponse}

import scala.scalajs.js

/**
  * Message Data Service
  * @author lawrence.daniels@gmail.com
  */
class MessageDataService($http: Http) extends Service {

  def getMessage(topic: String, partition: Int, offset: Int) = {
    $http.get[Message](s"/api/message_data/$topic/$partition/$offset")
  }

  def getMessageKey(topic: String, partition: Int, offset: Int) = {
    $http.get[Message](s"/api/message_key/$topic/$partition/$offset")
  }

  def publishMessage(topic: String, key: String, message: String, keyFormat: String, messageFormat: String) = {
    $http.post[PublishMessageResponse](
      url = s"/api/message/${encodeURI(topic)}",
      headers = js.Dictionary("Content-Type" -> "application/json"),
      data = js.Dictionary(
        "key" -> key,
        "message" -> message,
        "keyFormat" -> keyFormat,
        "messageFormat" -> messageFormat))
  }

}
