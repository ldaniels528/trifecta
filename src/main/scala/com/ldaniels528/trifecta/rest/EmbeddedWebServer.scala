package com.ldaniels528.trifecta.rest

import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.trifecta.io.zookeeper.ZKProxy
import com.ldaniels528.trifecta.rest.EmbeddedWebServer._
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.IOUtils
import org.mashupbots.socko.events.{HttpRequestEvent, HttpResponseStatus}
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes.{GET, Routes}
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(zk: ZKProxy, concurrency: Int = 5) extends Logger {
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(actorConfig))
  private var webServer: Option[WebServer] = None
  private val facade = new KafkaRestFacade(zk)

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = EmbeddedWebServer.this.stop()
  })

  // create the actors
  val actors = (1 to concurrency) map (_ => actorSystem.actorOf(Props(new WebContentHandler(facade))))
  var router = 0

  val routes = Routes({
    case GET(request) =>
      actors(router % actors.length) ! request
      router += 1
  })

  /**
   * Starts the embedded app server
   */
  def start() {
    if (webServer.isEmpty) {
      webServer = Option(new WebServer(WebServerConfig(), routes, actorSystem))
      webServer.foreach(_.start())
    }
  }

  /**
   * Stops the embedded app server
   */
  def stop() {
    Try(webServer.foreach(_.stop()))
    webServer = None
  }

}

/**
 * Embedded Web Server Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object EmbeddedWebServer {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private val DocumentRoot = "/app"
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
   * Web Content Handler
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class WebContentHandler(facade: KafkaRestFacade) extends Actor {
    def receive = {
      case event: HttpRequestEvent =>
        val endPoint = event.request.endPoint
        val response = event.response
        val path = translatePath(endPoint.path)

        path match {
          case s =>
            getContent(s) map { bytes =>
              getMimeType(s) foreach (response.contentType = _)
              logger.info(s"Retrieved resource '$s' (${bytes.length} bytes)")
              response.write(bytes)
            } getOrElse {
              logger.error(s"Resource '$path' not found")
              response.write(HttpResponseStatus.NOT_FOUND)
            }
        }
      //context.stop(self)
    }

    /**
     * Retrieves the given resource from the class path
     * @param path the given resource path (e.g. "/images/greenLight.png")
     * @return the option of an array of bytes representing the content
     */
    private def getContent(path: String): Option[Array[Byte]] = {
      path match {
        case s if s.startsWith(RestRoot) => processRestRequest(path)
        case s =>
          Resource(s) map { url =>
            new ByteArrayOutputStream(1024) use { out =>
              url.openStream() use (IOUtils.copy(_, out))
              out.toByteArray
            }
          }
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
        case "gif" => Some("image/gif")
        case "htm" | "html" => Some("text/html")
        case "jpg" | "jpeg" => Some("image/jpeg")
        case "js" => Some("text/javascript")
        case "json" => Some("application/json")
        case "png" => Some("image/png")
        case "xml" => Some("application/xml")
        case _ =>
          logger.warn(s"No MIME type found for '$path'")
          None
      }
    }

    private def processRestRequest(path: String): Option[Array[Byte]] = {
      import net.liftweb.json._

      // get the action and path arguments
      val params = path.indexOptionOf(RestRoot)
        .map(index => path.substring(index + RestRoot.length + 1))
        .map(_.split("[/]")) map (a => (a.head, a.tail.toList))

      // execute the action and get the JSON value
      val response: Option[JValue] = params flatMap { case (action, args) =>
        action match {
          case "findOne" => args match {
            case topic :: decoderURL :: criteria :: Nil => Option(facade.findOne(topic, decoderURL, criteria))
            case _ => None
          }
          case "getConsumers" if args.isEmpty => Option(facade.getConsumers)
          case "getConsumerMapping" if args.isEmpty => Option(facade.getConsumerMapping)
          case "getDecoders" if args.isEmpty => Option(facade.getDecoders)
          case "getMessage" => args match {
            case topic :: partition :: offset :: decoderURL :: Nil => Option(facade.getMessage(topic, partition.toInt, offset.toLong, Option(decoderURL)))
            case topic :: partition :: offset :: Nil => Option(facade.getMessage(topic, partition.toInt, offset.toLong))
            case _ => None
          }
          case "getTopicByName" => args match {
            case name :: Nil => facade.getTopicByName(name)
            case _ => None
          }
          case "getTopicDetailsByName" => args match {
            case name :: Nil => Option(facade.getTopicDetailsByName(name))
            case _ => None
          }
          case "getTopics" if args.isEmpty => Option(facade.getTopics)
          case "getTopicSummaries" if args.isEmpty => Option(facade.getTopicSummaries)
          case _ => None
        }
      }

      // convert the JSON value to a binary array
      response map (js => compact(render(js))) map (_.getBytes)
    }

    /**
     * Translates the given logical path to a physical path
     * @param path the given logical path
     * @return the corresponding physical path
     */
    private def translatePath(path: String) = path match {
      case s if s.startsWith(RestRoot) => s
      case "/" => s"$DocumentRoot/index.htm"
      case s => s"$DocumentRoot$s"
    }
  }

}
