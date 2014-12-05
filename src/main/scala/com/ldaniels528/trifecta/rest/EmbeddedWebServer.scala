package com.ldaniels528.trifecta.rest

import java.io.{ByteArrayOutputStream, File}

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.trifecta.TxConfig
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.typesafe.config.ConfigFactory
import net.liftweb.json._
import org.apache.commons.io.IOUtils
import org.mashupbots.socko.events.{HttpRequestEvent, HttpResponseStatus, WebSocketFrameEvent}
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.util.Try

/**
 * Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(config: TxConfig, zk: ZKProxy) extends Logger {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(actorConfig))
  private val facade = new KafkaRestFacade(config, zk)
  private val sessions = TrieMap[String, String]()
  private var webServer: Option[WebServer] = None

  // create the web content actors
  private var wcRouter = 0
  private val wcActors = (1 to config.queryConcurrency) map (_ => actorSystem.actorOf(Props(new WebContentHandler(facade))))

  // create the web socket actors
  private var wsRouter = 0
  private val wsActors = (1 to config.queryConcurrency) map (_ => actorSystem.actorOf(Props(new WebSocketHandler(facade))))

  // create the actor references
  private def wcActor = wcActors(wcRouter % wcActors.length) and (_ => wcRouter += 1)

  private def wsActor = wsActors(wsRouter % wsActors.length) and (_ => wsRouter += 1)

  // define all of the routes
  val routes = Routes({
    case HttpRequest(request) => wcActor ! request
    case WebSocketHandshake(wsHandshake) => wsHandshake match {
      case Path("/websocket/") =>
        logger.info(s"Authorizing websocket handshake...")
        wsHandshake.authorize(
          onComplete = Some(onWebSocketHandshakeComplete),
          onClose = Some(onWebSocketClose))
    }
    case WebSocketFrame(wsFrame) => wsActor ! wsFrame
  })

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = {
      EmbeddedWebServer.this.stop()
    }
  })

  /**
   * Starts the embedded app server
   */
  def start() {
    if (webServer.isEmpty) {
      webServer = Option(new WebServer(WebServerConfig(hostname = config.webHost, port = config.webPort), routes, actorSystem))
      webServer.foreach(_.start())

      // setup event management
      implicit val ec = actorSystem.dispatcher
      actorSystem.scheduler.schedule(initialDelay = 5.seconds, interval = 5.seconds)(handleWebSocketPushEvents())
    }
  }

  /**
   * Stops the embedded app server
   */
  def stop() {
    Try(webServer.foreach(_.stop()))
    webServer = None
  }

  private def onWebSocketHandshakeComplete(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId connected")
    sessions += webSocketId -> webSocketId // TODO do we need to a session instance for tracking?
  }

  private def onWebSocketClose(webSocketId: String) {
    logger.info(s"Web Socket $webSocketId closed")
    sessions -= webSocketId
  }

  /**
   * Manages pushing events to connected web-socket clients
   */
  private def handleWebSocketPushEvents() {
    if (sessions.nonEmpty) {
      pushConsumerUpdateEvents()
      pushTopicUpdateEvents()
    }
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushConsumerUpdateEvents() {
    val deltas = facade.getConsumerDeltas
    if (deltas.nonEmpty) {
      logger.info(s"pushConsumerUpdateEvents: Retrieved ${deltas.length} consumer(s)...")
      val deltasJs = JsonHelper.makeCompact(deltas)
      webServer.foreach(_.webSocketConnections.writeText(deltasJs))
    }
  }

  /**
   * Pushes topic update events to connected web-socket clients
   */
  private def pushTopicUpdateEvents() {
    val deltas = facade.getTopicDeltas
    if (deltas.nonEmpty) {
      logger.info(s"pushTopicUpdateEvents: Retrieved ${deltas.length} topic(s)...")
      val deltasJs = JsonHelper.makeCompact(deltas)
      webServer.foreach(_.webSocketConnections.writeText(deltasJs))
    }
  }

}

/**
 * Embedded Web Server Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EmbeddedWebServer {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val DocumentRoot = "/app"
  private val StreamingRoot = "/stream"
  private val RestRoot = "/rest"
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

  /**
   * Web Content Handling Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class WebContentHandler(facade: KafkaRestFacade) extends Actor {
    def receive = {
      case event: HttpRequestEvent =>
        val startTime = System.nanoTime()
        val request = event.request
        val response = event.response
        val path = translatePath(request.endPoint.path)

        // send 100 continue if required
        if (event.request.is100ContinueExpected) {
          event.response.write100Continue()
        }

        getContent(path, request.content.toFormDataMap) map { case (mimeType, bytes) =>
          val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
          logger.info(f"Retrieved resource '$path' (${bytes.length} bytes) [$elapsedTime%.1f msec]")
          response.contentType = mimeType
          response.write(bytes)
        } getOrElse {
          logger.error(s"Resource '$path' not found")
          response.write(HttpResponseStatus.NOT_FOUND)
        }
      //context.stop(self)
    }

    /**
     * Retrieves the given resource from the class path
     * @param path the given resource path (e.g. "/images/greenLight.png")
     * @return the option of an array of bytes representing the content
     */
    private def getContent(path: String, dataMap: Map[String, List[String]]): Option[(String, Array[Byte])] = {
      path match {
        // REST requests
        case s if s.startsWith(RestRoot) =>
          for {
            bytes <- processRestRequest(path, dataMap)
            mimeType <- getMimeType(s)
          } yield mimeType -> bytes

        // streaming data requests
        case s if s.startsWith(StreamingRoot) => None

        // resource requests
        case s =>
          for {
            bytes <- Resource(s) map (url =>
              new ByteArrayOutputStream(1024) use { out =>
                url.openStream() use (IOUtils.copy(_, out))
                out.toByteArray
              })
            mimeType <- getMimeType(s)
          } yield mimeType -> bytes
      }
    }

    /**
     * Returns the MIME type for the given resource
     * @param path the given resource path (e.g. "/images/greenLight.png")
     * @return the option of a MIME type (e.g. "image/png")
     */
    private def getMimeType(path: String): Option[String] = {
      if (path.startsWith(RestRoot)) Some("application/json")
      else path.lastIndexOptionOf(".") map (index => path.substring(index + 1)) flatMap {
        case "css" => Some("text/css")
        case "csv" => Some("text/csv")
        case "gif" => Some("image/gif")
        case "htm" | "html" => Some("text/html")
        case "jpg" | "jpeg" => Some("image/jpeg")
        case "js" => Some("text/javascript")
        case "json" => Some("application/json")
        case "map" => Some("application/json")
        case "png" => Some("image/png")
        case "xml" => Some("application/xml")
        case _ =>
          logger.warn(s"No MIME type found for '$path'")
          None
      }
    }

    private def processRestRequest(path: String, dataMap: Map[String, List[String]]) = {
      import java.net.URLDecoder.decode

      // get the action and path arguments
      val params = path.indexOptionOf(RestRoot)
        .map(index => path.substring(index + RestRoot.length + 1))
        .map(_.split("[/]")) map (a => (a.head, a.tail.toList))

      params foreach { case (action, args) => logger.info(s"processRestRequest Action: $action ~> values = $args")}
      dataMap foreach { case (key, values) => logger.info(s"processRestRequest Content: '$key' ~> values = $values")}

      // execute the action and get the JSON value
      val response: Option[JValue] = params flatMap { case (action, args) =>
        action match {
          case "downloadResults" => args match {
            case queryString :: Nil => Option(facade.downloadResults(decode(queryString, "UTF8")))
            case _ => None
          }
          case "executeQuery" => args match {
            case queryString :: Nil => Option(facade.executeQuery(decode(queryString, "UTF8")))
            case _ => None
          }
          case "findOne" => args match {
            case topic :: criteria :: Nil => Option(facade.findOne(topic, decode(criteria, "UTF8")))
            case _ => None
          }
          case "getConsumerDeltas" if args.isEmpty => Option(JsonHelper.toJson(facade.getConsumerDeltas))
          case "getConsumers" if args.isEmpty => Option(facade.getConsumers)
          case "getConsumerSet" if args.isEmpty => Option(facade.getConsumerSet)
          case "getLastQuery" if args.isEmpty => Option(facade.getLastQuery)
          case "getMessage" => args match {
            case topic :: partition :: offset :: Nil => Option(facade.getMessage(topic, partition.toInt, offset.toLong))
            case _ => None
          }
          case "getTopicByName" => args match {
            case name :: Nil => facade.getTopicByName(name)
            case _ => None
          }
          case "getTopicDeltas" if args.isEmpty => Option(JsonHelper.toJson(facade.getTopicDeltas))
          case "getTopicDetailsByName" => args match {
            case name :: Nil => Option(facade.getTopicDetailsByName(name))
            case _ => None
          }
          case "getTopics" if args.isEmpty => Option(facade.getTopics)
          case "getTopicSummaries" if args.isEmpty => Option(facade.getTopicSummaries)
          case "getZkData" => Option(facade.getZkData(toZkPath(args.init), args.last))
          case "getZkInfo" => Option(facade.getZkInfo(toZkPath(args)))
          case "getZkPath" => Option(facade.getZkPath(toZkPath(args)))
          case _ => None
        }
      }

      // convert the JSON value to a binary array
      response map (js => compact(render(js))) map (_.getBytes)
    }

    private def toZkPath(args: List[String]): String = "/" + args.mkString("/")

    /**
     * Translates the given logical path to a physical path
     * @param path the given logical path
     * @return the corresponding physical path
     */
    private def translatePath(path: String) = path match {
      case "/" | "/index.htm" | "/index.html" => s"$DocumentRoot/index.htm"
      case s => s
    }
  }

  /**
   * Web Socket Handling Actor
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class WebSocketHandler(facade: KafkaRestFacade) extends Actor {
    def receive = {
      case event: WebSocketFrameEvent =>
        writeWebSocketResponse(event)
        context.stop(self)
      case message =>
        logger.info(s"received unknown message of type: $message")
        unhandled(message)
        context.stop(self)
    }

    /**
     * Echo the details of the web socket frame that we just received; but in upper case.
     */
    private def writeWebSocketResponse(event: WebSocketFrameEvent) {
      logger.info(s"TextWebSocketFrame: ${event.readText()}")
    }
  }

  /**
   * TxConfig Extensions
   * @param config the given [[TxConfig]]
   */
  implicit class TxConfigExtensions(val config: TxConfig) extends AnyVal {

    /**
     * Returns the location of the queries file
     * @return the [[File]] representing the location of queries file
     */
    def queriesFile: File = new File(TxConfig.trifectaPrefs, "queries.txt")

    /**
     * Returns the query execution concurrency
     * @return the query execution concurrency
     */
    def queryConcurrency: Int = config.getOrElse("trifecta.query.concurrency", "10").toInt

  }

}
