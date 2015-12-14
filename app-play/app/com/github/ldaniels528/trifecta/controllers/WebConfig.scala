package com.github.ldaniels528.trifecta.controllers

import java.util.concurrent.atomic.AtomicBoolean

import com.github.ldaniels528.trifecta.TxConfig
import com.github.ldaniels528.trifecta.io.zookeeper.ZKProxy
import play.api.Logger

import scala.concurrent.ExecutionContext
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
  lazy val config = Try(TxConfig.load(TxConfig.configFile)) match {
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
  lazy val facade = new KafkaRestFacade(config, zk)

  /**
    * Initializes the config
    */
  def init(implicit ec: ExecutionContext) = {
    Logger.info("Initializing Trifecta REST facade...")
    if(once.compareAndSet(true, false)) facade.init
  }

}
