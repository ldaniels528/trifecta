package com.github.ldaniels528.trifecta.actors

import akka.actor.ActorRef

/**
  * Represents a Server-Side Events session
  * @param sessionId the given [[String session ID]]
  * @param actor the given [[ActorRef actor]]
  * @author lawrence.daniels@gmail.com
  */
case class SSESession(sessionId: String, actor: ActorRef)
