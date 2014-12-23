package com.ldaniels528.trifecta.rest

import akka.actor.{ActorSystem, Props}
import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.rest.PushEventActor._
import com.ldaniels528.trifecta.rest.TxWebConfig._
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.typesafe.config.ConfigFactory
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.util.Try

/**
 * Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(config: TxConfig, zk: ZKProxy) extends Logger {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(getActorConfig))
  private val facade = new KafkaRestFacade(config, zk)
  private val sessionMgr = new WebSocketSessionManager()

  implicit val ec = actorSystem.dispatcher

  // define all of the routes
  val routes = Routes({
    case HttpRequest(request) => wcActor ! request
    case WebSocketHandshake(wsHandshake) => wsHandshake match {
      case Path("/websocket/") =>
        logger.info(s"Authorizing web socket handshake...")
        wsHandshake.authorize(
          onComplete = Option(sessionMgr.onWebSocketHandshakeComplete),
          onClose = Option(sessionMgr.onWebSocketClose))
    }
    case WebSocketFrame(wsFrame) => wsActor ! wsFrame
  })

  // create the web service instance
  private val webServer = new WebServer(WebServerConfig(hostname = config.webHost, port = config.webPort), routes, actorSystem)

  // create the web content actors
  private var wcRouter = 0
  private val wcActors = (1 to config.webActorConcurrency) map (_ => actorSystem.actorOf(Props(new WebContentActor(facade))))

  // create the web socket actors
  private var wsRouter = 0
  private val wsActors = (1 to config.webActorConcurrency) map (_ => actorSystem.actorOf(Props(new WebSocketActor(facade))))

  // create the push event actors
  private var pushRouter = 0
  private val pushActors = (1 to 2) map (_ => actorSystem.actorOf(Props(new PushEventActor(sessionMgr, webServer, facade))))

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
      if (sessionMgr.sessionsExist) {
        pushActor ! PushConsumers
      }
    }

    // setup topic update push events
    actorSystem.scheduler.schedule(initialDelay = 5.seconds, interval = config.topicPushInterval.seconds) {
      if (sessionMgr.sessionsExist) {
        pushActor ! PushTopics
      }
    }
    ()
  }

  /**
   * Stops the embedded app server
   */
  def stop() {
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

}
