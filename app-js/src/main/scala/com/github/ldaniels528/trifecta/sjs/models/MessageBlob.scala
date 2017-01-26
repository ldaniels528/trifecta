package com.github.ldaniels528.trifecta.sjs.models

import io.scalajs.util.ScalaJsHelper._

import scala.scalajs.js

/**
  * Message Blob
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait MessageBlob extends js.Object {
  var topic: js.UndefOr[TopicDetails] = js.native
  var key: js.UndefOr[String] = js.native
  var keyFormat: js.UndefOr[String] = js.native
  var keyAuto: js.UndefOr[Boolean] = js.native
  var message: js.UndefOr[String] = js.native
  var messageFormat: js.UndefOr[String] = js.native
}

/**
  * Message Blob Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MessageBlob {

  def apply(topic: js.UndefOr[TopicDetails] = js.undefined,
            key: js.UndefOr[String] = js.undefined,
            keyFormat: js.UndefOr[String] = js.undefined,
            keyAuto: js.UndefOr[Boolean] = js.undefined,
            message: js.UndefOr[String] = js.undefined,
            messageFormat: js.UndefOr[String] = js.undefined) = {
    val blob = New[MessageBlob]
    blob.topic = topic
    blob.key = key
    blob.keyFormat = keyFormat
    blob.keyAuto = keyAuto
    blob.message = message
    blob.messageFormat = messageFormat
    blob
  }

  /**
    * MessageBlob validation
    * @param blob the given [[MessageBlob blob]]
    */
  implicit class MessageBlobValidation(val blob: MessageBlob) extends AnyVal {

    @inline
    def validate = {
      val messages = new js.Array[String]()
      if (!blob.topic.exists(!_.topic.isEmpty)) {
        messages.push("No topic specified")
      }
      if (!blob.keyFormat.exists(!_.isEmpty)) {
        messages.push("No message key format specified")
      }
      if (!blob.message.exists(!_.isEmpty)) {
        messages.push("No message body specified")
      }
      if (!blob.messageFormat.exists(!_.isEmpty)) {
        messages.push("No message body format specified")
      }
      messages
    }

  }

}