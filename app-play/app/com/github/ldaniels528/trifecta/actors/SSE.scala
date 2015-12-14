package com.github.ldaniels528.trifecta.actors

import akka.actor.ActorRef
import play.api.Logger
import play.api.libs.iteratee.Concurrent
import play.api.libs.json.{JsValue, Json}
import play.libs.Akka

import scala.collection.concurrent.TrieMap

/**
  * Server-Side Events (SSE) Actor Proxy
  * @author lawrence.daniels@gmail.com
  */
object SSE {
  private[this] val sessions = TrieMap[String, SSESession]()

  /**
    * Establishes an SSE connection; launching an actor to support the given user
    * @param sessionID the given session ID
    * @param sseOutChannel the given [[Concurrent.Channel channel]]
    * @return a newly created [[ActorRef actor]]
    */
  def connect(sessionID: String, sseOutChannel: Concurrent.Channel[JsValue]): ActorRef = {
    Akka.system.actorOf(SSEClientHandlingActor.props(sessionID, sseOutChannel))
  }

  /**
    * Links a new SSE session in the session registry
    * @param session the given [[SSESession session]]
    */
  def link(session: SSESession) = {
    Logger.info(s"Registering new SSE session '${session.sessionId}'...")
    sessions.put(session.sessionId, session)
    ()
  }

  /**
    * Removes a session from the session registry
    * @param sessionId the given [[String session ID]]
    * @return an option of the removed session
    */
  def unlink(sessionId: String) = {
    Logger.info(s"Discarding SSE session '$sessionId'...")
    sessions.remove(sessionId)
  }

  /**
    * Sends the given JSON message to the relay actor pool
    * @param message the given [[SSEMessage SSE message]]
    */
  def !(message: SSEMessage): Unit = {
    sessions.foreach { case (_, SSESession(_, actor)) =>
      actor ! Json.toJson(message)
    }
  }

}
