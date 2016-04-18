package com.github.ldaniels528.trifecta.modules.etl

import java.io.File

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/**
  * Story Processor
  * @author lawrence.daniels@gmail.com
  */
class StoryProcessor() {
  private val logger = LoggerFactory.getLogger(getClass)

  def load(configFile: File) = {
    logger.info(s"Loading Broadway story config '${configFile.getAbsolutePath}'...")
    StoryConfigParser.parse(configFile)
  }

  def run(configFile: File)(implicit ec: ExecutionContext): Unit = load(configFile) foreach run

  /**
    * Executes the ETL processing
    * @param story the given [[StoryConfig ETL configuration]]
    */
  def run(story: StoryConfig)(implicit ec: ExecutionContext) {
    logger.info(s"Executing story '${story.id}'...")
    story.triggers foreach (_.execute(story))

    Thread.sleep(1.seconds.toMillis)
    logger.info("*" * 30 + " PROCESS COMPLETED " + "*" * 30)
  }

}
