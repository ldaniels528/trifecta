package com.github.ldaniels528.trifecta.modules.etl.io.trigger.impl

import com.github.ldaniels528.trifecta.modules.etl.StoryConfig
import com.github.ldaniels528.trifecta.modules.etl.io.flow.Flow
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.Trigger

import scala.concurrent.ExecutionContext

/**
  * Startup Trigger
  * @author lawrence.daniels@gmail.com
  */
case class StartupTrigger(id: String, flows: Seq[Flow]) extends Trigger {

  override def destroy() = {}

  override def execute(story: StoryConfig)(implicit ec: ExecutionContext) = {
    Trigger.taskPool ! new Runnable {
      override def run() = {
        process(story, flows zip (flows map (createScope(story, _))))
        ()
      }
    }
  }

}
