package com.github.ldaniels528.trifecta.ui.controllers

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.Props
import com.github.ldaniels528.trifecta.TxConfig
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.github.ldaniels528.trifecta.ui.actors.ReactiveEventsActor
import play.api.Logger
import play.libs.Akka

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Trifecta Web Configuration Singleton
  * @author lawrence.daniels@gmail.com
  */
object WebConfig {
  private val reactiveActor = Akka.system.actorOf(Props[ReactiveEventsActor])
  private val once = new AtomicBoolean(true)
  // TODO make this an injectable class

  // load the configuration
  Logger.info("Loading Trifecta configuration...")
  lazy val config: TxConfig = Try(TxConfig.load(TxConfig.configFile)) match {
    case Success(cfg) => cfg
    case Failure(e) =>
      val cfg = TxConfig.defaultConfig
      if (!TxConfig.configFile.exists()) {
        Logger.info(s"Creating default configuration file (${TxConfig.configFile.getAbsolutePath})...")
        cfg.save(TxConfig.configFile)
      }
      cfg
  }

  // connect to Zookeeper
  Logger.info("Starting Zookeeper client...")
  lazy val zk = ZKProxy(config.zooKeeperConnect)

  // create the REST API facade
  lazy val facade = new KafkaPlayRestFacade(config, zk)

  def getTopicPushInterval: FiniteDuration = {
    Try(config.getOrElse("trifecta.web.push.interval.topic", "60").toInt).getOrElse(60).seconds
  }

  def getTopicOffsetsPushInterval: FiniteDuration = {
    Try(config.getOrElse("trifecta.web.push.interval.topic.offsets", "30").toInt).getOrElse(30).seconds
  }

  def getConsumerPushInterval: FiniteDuration = {
    Try(config.getOrElse("trifecta.web.push.interval.topic", "60").toInt).getOrElse(60).seconds
  }

  /**
    * Initializes the config
    */
  def init(implicit ec: ExecutionContext): Unit = {
    if (once.compareAndSet(true, false)) {
      Logger.info("Initializing Trifecta REST facade...")
      facade.init

      // schedule streaming updates
      Logger.info("Scheduling streaming updates...")
      //reactiveActor ! StreamingConsumerUpdateRequest(WebConfig.getConsumerPushInterval)
      //reactiveActor ! StreamingTopicUpdateRequest(WebConfig.getTopicPushInterval)
    }
  }

}
