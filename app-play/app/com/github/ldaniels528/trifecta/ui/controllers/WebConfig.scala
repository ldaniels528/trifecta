package com.github.ldaniels528.trifecta.ui.controllers

import java.util.concurrent.atomic.AtomicBoolean

import com.github.ldaniels528.trifecta.TxConfig
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Trifecta Web Configuration Singleton
  * @author lawrence.daniels@gmail.com
  */
object WebConfig {
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

  def getTopicOffsetsPushInterval: Option[FiniteDuration] = {
    config.get("trifecta.web.push.interval.topic") flatMap (value => Try(value.toInt.seconds).toOption)
  }

  def getConsumerOffsetsPushInterval: Option[FiniteDuration] = {
    config.get("trifecta.web.push.interval.consumer") flatMap (value => Try(value.toInt.seconds).toOption)
  }

  /**
    * Initializes the config
    */
  def init(implicit ec: ExecutionContext): Unit = {
    if (once.compareAndSet(true, false)) {
      Logger.info("Initializing Trifecta REST facade...")
      facade.init
    }
  }

}
