package com.ldaniels528.trifecta.rest

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.rest.PushEventActor._
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.typesafe.config.ConfigFactory
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.io.Source
import scala.util.Try

/**
 * Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(config: TxConfig, zk: ZKProxy) extends Logger {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(actorConfig))
  private val facade = new KafkaRestFacade(config, zk)
  private val sessions = TrieMap[String, WebSocketSession]()

  // define all of the routes
  val routes = Routes({
    case HttpRequest(request) => wcActor ! request
    case WebSocketHandshake(wsHandshake) => wsHandshake match {
      case Path("/websocket/") =>
        logger.info(s"Authorizing websocket handshake...")
        wsHandshake.authorize(
          onComplete = Option(onWebSocketHandshakeComplete),
          onClose = Option(onWebSocketClose))
    }
    case WebSocketFrame(wsFrame) => wsActor ! wsFrame
  })

  // create the web service instance
  private val webServer = new WebServer(WebServerConfig(hostname = config.webHost, port = config.webPort), routes, actorSystem)

  // create the web content actors
  private var wcRouter = 0
  private val wcActors = (1 to config.queryConcurrency) map (_ => actorSystem.actorOf(Props(new WebContentActor(facade))))

  // create the web socket actors
  private var wsRouter = 0
  private val wsActors = (1 to config.queryConcurrency) map (_ => actorSystem.actorOf(Props(new WebSocketActor(facade))))

  // create the push event actors
  private var pushRouter = 0
  private val pushActors = (1 to 2) map (_ => actorSystem.actorOf(Props(new PushEventActor(webServer, facade))))

  // create the actor references
  private def wcActor = wcActors(wcRouter % wcActors.length) and (_ => wcRouter += 1)

  private def wsActor = wsActors(wsRouter % wsActors.length) and (_ => wsRouter += 1)

  private def pushActor = pushActors(pushRouter % pushActors.length) and (_ => pushRouter += 1)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = {
      EmbeddedWebServer.this.stop()
    }
  })

  /**
   * Starts the embedded app server
   */
  def start() {
    implicit val ec = actorSystem.dispatcher
    webServer.start()

    // setup consumer update push events
    actorSystem.scheduler.schedule(initialDelay = 5.seconds, interval = config.consumerPushInterval.seconds) {
      if (sessions.nonEmpty) {
        pushActor ! PushConsumers
      }
    }

    // setup topic update push events
    actorSystem.scheduler.schedule(initialDelay = 5.seconds, interval = config.topicPushInterval.seconds) {
      if (sessions.nonEmpty) {
        pushActor ! PushTopics
      }
    }
  }

  /**
   * Stops the embedded app server
   */
  def stop() {
    Try(webServer.stop())
    ()
  }

  private def onWebSocketHandshakeComplete(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId connected")
    sessions += webSocketId -> WebSocketSession(webSocketId)
  }

  private def onWebSocketClose(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId closed")
    sessions -= webSocketId
  }

}

/**
 * Embedded Web Server Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EmbeddedWebServer {

  private val actorConfig = """
      my-pinned-dispatcher {
        type=PinnedDispatcher
        executor=thread-pool-executor
      }
      akka {
        event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
        loglevel=DEBUG
        actor {
          deployment {
            /static-file-router {
              router = round-robin
              nr-of-instances = 5
            }
            /file-upload-router {
              router = round-robin
              nr-of-instances = 5
            }
          }
        }
      }"""

  case class TxQuery(name: String, queryString: String, exists: Boolean, lastModified: Long)

  case class WebSocketSession(webSocketId: String, var requests: Long = 0)

  /**
   * Trifecta Web Configuration
   * @param config the given [[TxConfig]]
   */
  implicit class TxWebConfig(val config: TxConfig) extends AnyVal {

    def getQueries: Option[Array[TxQuery]] = {
      def removeExtension(name: String) = name.lastIndexOptionOf(".bdql") match {
        case Some(index) => name.substring(0, index)
        case None => name
      }

      Option(queriesDirectory.listFiles) map { queriesFiles =>
        queriesFiles map { file =>
          val name = removeExtension(file.getName)
          TxQuery(name, Source.fromFile(file).getLines().mkString("\n"), file.exists(), file.lastModified())
        }
      }
    }

    /**
     * Returns the push interval (in seconds) for topic changes
     * @return the interval
     */
    def topicPushInterval: Int = config.getOrElse("trifecta.rest.push.interval.topic", "15").toInt

    /**
     * Returns the push interval (in seconds) for consumer offset changes
     * @return the interval
     */
    def consumerPushInterval: Int = config.getOrElse("trifecta.rest.push.interval.consumer", "15").toInt

    /**
     * Returns the location of the queries directory
     * @return the [[File]] representing the location of the queries directory
     */
    def queriesDirectory: File = new File(TxConfig.trifectaPrefs, "queries")

    /**
     * Returns the query execution concurrency
     * @return the query execution concurrency
     */
    def queryConcurrency: Int = config.getOrElse("trifecta.query.concurrency", "10").toInt

  }

}
