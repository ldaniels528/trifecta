package com.github.ldaniels528.trifecta.util

import java.util.concurrent.ConcurrentLinkedQueue

import org.slf4j.LoggerFactory

/**
  * Represents a resource pool
  * @author lawrence.daniels@gmail.com
  */
case class ResourcePool[V](makeNew: () => V) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val pool = new ConcurrentLinkedQueue[V]()

  def give(resource: V): Unit = {
    pool.add(resource)
    ()
  }

  def take: V = {
    Option(pool.poll()) match {
      case Some(resource) => resource
      case None =>
        logger.info("Creating a new resource...")
        makeNew()
    }
  }

}
