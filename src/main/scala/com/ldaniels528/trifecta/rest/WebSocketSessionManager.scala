package com.ldaniels528.trifecta.rest

import com.ldaniels528.trifecta.rest.WebSocketSessionManager._
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Web Socket Session Manager
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebSocketSessionManager() {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val sessions = TrieMap[String, WebSocketSession]()

  def sessionsExist: Boolean = sessions.nonEmpty

  def sessionsCount: Int = sessions.size

  def onWebSocketHandshakeComplete(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId connected")
    sessions += webSocketId -> WebSocketSession(webSocketId)
    ()
  }

  def onWebSocketClose(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId closed")
    sessions -= webSocketId
    ()
  }

}

/**
 * Web Socket Session Manager Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object WebSocketSessionManager {

  case class WebSocketSession(webSocketId: String, var requests: Long = 0)

}
