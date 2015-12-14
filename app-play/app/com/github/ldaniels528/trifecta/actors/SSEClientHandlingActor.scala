package com.github.ldaniels528.trifecta.actors

import akka.actor.{Actor, ActorLogging, Props}
import play.api.libs.iteratee.Concurrent
import play.api.libs.json.JsValue

/**
  * SSE Client Handling Actor
  * @author lawrence.daniels@gmail.com
  */
class SSEClientHandlingActor(sessionId: String, outChannel: Concurrent.Channel[JsValue]) extends Actor with ActorLogging {

  override def preStart() = SSE.link(SSESession(sessionId, self))

  override def postStop() = SSE.unlink(sessionId)

  override def receive = {
    case message: JsValue =>
      outChannel.push(message)

    case message =>
      log.warning(s"Unhandled message $message")
      unhandled(message)
  }
}

/**
  * Web Socket Handling Actor Companion
  * @author lawrence.daniels@gmail.com
  */
object SSEClientHandlingActor {

  def props(sessionId: String, outChannel: Concurrent.Channel[JsValue]) = Props(new SSEClientHandlingActor(sessionId, outChannel))

}