package com.github.ldaniels528.trifecta.modules.etl

import java.io.FileInputStream

import com.github.ldaniels528.commons.helpers.ResourceHelper._
import com.github.ldaniels528.trifecta.modules.etl.StoryConfig.StoryProperties
import com.github.ldaniels528.trifecta.modules.etl.io.Scope
import com.github.ldaniels528.trifecta.modules.etl.io.archive.Archive
import com.github.ldaniels528.trifecta.modules.etl.io.device.DataSource
import com.github.ldaniels528.trifecta.modules.etl.io.filters.Filter
import com.github.ldaniels528.trifecta.modules.etl.io.layout.Layout
import com.github.ldaniels528.trifecta.modules.etl.io.trigger.Trigger

import scala.collection.JavaConversions._

/**
  * Story Configuration
  * @author lawrence.daniels@gmail.com
  */
case class StoryConfig(id: String,
                       archives: Seq[Archive] = Nil,
                       devices: Seq[DataSource] = Nil,
                       filters: Seq[(String, Filter)] = Nil,
                       layouts: Seq[Layout] = Nil,
                       properties: Seq[StoryProperties] = Nil,
                       triggers: Seq[Trigger] = Nil)

/**
  * Story Configuration Companion Object
  */
object StoryConfig {

  /**
    * Story Configuration Properties
    */
  trait StoryProperties {

    def load(implicit scope: Scope): Seq[(String, String)]

  }

  /**
    * Story Configuration Properties File
    */
  case class StoryPropertiesFile(path: String) extends StoryProperties {

    override def load(implicit scope: Scope) = {
      new FileInputStream(scope.evaluateAsString(path)) use { in =>
        val props = new java.util.Properties()
        props.load(in)
        props.toSeq
      }
    }

  }

  /**
    * Story Configuration Properties Sequence
    */
  case class StoryPropertySeq(properties: Seq[(String, String)]) extends StoryProperties {

    override def load(implicit scope: Scope) = properties

  }

}