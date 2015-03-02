package com.ldaniels528.trifecta.rest

import akka.actor.{ActorSystem, Props}
import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.rest.KafkaRestFacade.{SamplingCursor, TopicAndPartitions}
import com.ldaniels528.trifecta.rest.PushEventActor._
import com.ldaniels528.trifecta.rest.TxWebConfig._
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.typesafe.config.ConfigFactory
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util.Try

/**
 * Trifecta Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(config: TxConfig, zk: ZKProxy) extends Logger {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(getActorConfig))
  private val sessions = TrieMap[String, WebSocketSession]()
  val sampling = TrieMap[String, TopicAndPartitions]()
  val facade = new KafkaRestFacade(config, zk)

  implicit val ec = actorSystem.dispatcher

  // define all of the routes
  private val routes = Routes({
    case HttpRequest(request) => wcActor ! request
    case WebSocketHandshake(wsHandshake) => wsHandshake match {
      case Path("/websocket/") =>
        logger.info(s"Authorizing web socket handshake...")
        wsHandshake.authorize(
          onComplete = Option(onWebSocketHandshakeComplete),
          onClose = Option(onWebSocketClose))
    }
    case WebSocketFrame(wsFrame) => wsActor ! wsFrame
  })

  // create the web service instance
  val webServer = new WebServer(WebServerConfig(hostname = config.webHost, port = config.webPort), routes, actorSystem)

  // create the web content actors
  private var wcRouter = 0
  private val wcActors = (1 to config.webActorConcurrency) map (_ => actorSystem.actorOf(Props(new WebContentActor(facade))))

  // create the web socket actors
  private var wsRouter = 0
  private val wsActors = (1 to config.webActorConcurrency) map (_ => actorSystem.actorOf(Props(new WebSocketActor(this))))

  // create the push event actors
  private var pushRouter = 0
  private val pushActors = (1 to 2) map (_ => actorSystem.actorOf(Props(new PushEventActor(this))))

  // create the actor references
  private def wcActor = wcActors(wcRouter % wcActors.length) and (_ => wcRouter += 1)

  private def wsActor = wsActors(wsRouter % wsActors.length) and (_ => wsRouter += 1)

  private def pushActor = pushActors(pushRouter % pushActors.length) and (_ => pushRouter += 1)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = {
      EmbeddedWebServer.this.stopServer()
    }
  })

  /**
   * Retrieves a session by the client's web socket ID
   * @param webSocketId the client's web socket ID
   * @return an option of a [[WebSocketSession]]
   */
  def getSession(webSocketId: String) = sessions.get(webSocketId)

  def onWebSocketHandshakeComplete(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId connected")
    sessions += webSocketId -> WebSocketSession(webSocketId)
    ()
  }

  def onWebSocketClose(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId closed")
    sessions -= webSocketId
    sampling -= webSocketId
    ()
  }

  /**
   * Starts the embedded app server
   */
  def startServer() {
    implicit val ec = actorSystem.dispatcher
    webServer.start()

    // setup consumer update push events
    actorSystem.scheduler.schedule(initialDelay = 0.seconds, interval = config.consumerPushInterval.seconds) {
      if (sessions.nonEmpty) {
        pushActor ! PushConsumers
      }
    }

    // setup topic update push events
    actorSystem.scheduler.schedule(initialDelay = 0.seconds, interval = config.topicPushInterval.seconds) {
      if (sessions.nonEmpty) {
        pushActor ! PushTopics
      }
    }

    // setup message sampling push events
    actorSystem.scheduler.schedule(initialDelay = 0.seconds, interval = config.samplingPushInterval.seconds) {
      if (sessions.nonEmpty) {
        sampling foreach { case (webSocketId, topicAndPartitions) =>
          pushActor ! PushMessage(webSocketId, topicAndPartitions)
        }
      }
    }
    ()
  }

  def startMessageSampling(webSocketId: String, topic: String, partitions: Seq[Long]): Unit = {
    logger.info(s"Starting subscription for message updates for topic '$topic'")
    sessions.get(webSocketId) foreach { session =>
      sampling += webSocketId -> TopicAndPartitions(topic, partitions)
    }
  }

  def stopMessageSampling(webSocketId: String, topic: String): Unit = {
    logger.info(s"Ending subscription for message updates for topic '$topic'")
    sessions.get(webSocketId) foreach { session =>
      sampling -= webSocketId
    }
  }

  /**
   * Stops the embedded app server
   */
  def stopServer() {
    Try(webServer.stop())
    ()
  }

}

/**
 * Embedded Web Server Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EmbeddedWebServer {

  private def getActorConfig = """
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
              nr-of-instances = 10
            }
            /file-upload-router {
              router = round-robin
              nr-of-instances = 1
            }
          }
        }
      }"""

  case class WebSocketSession(webSocketId: String) {
    val cursors = new TrieMap[String, SamplingCursor]() // topic -> SamplingCursor
  }

}
