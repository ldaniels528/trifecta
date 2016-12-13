package com.github.ldaniels528.trifecta.modules.etl.actors

import akka.actor.ActorSystem

/**
  * Broadway Actor System
  * @author lawrence.daniels@gmail.com
  */
object BroadwayActorSystem {
  val system = ActorSystem("broadway_tasks")
  val scheduler = system.scheduler

  def shutdown() = system.shutdown()

}
