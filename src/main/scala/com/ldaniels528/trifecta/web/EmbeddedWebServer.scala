package com.ldaniels528.trifecta.web

import java.io.ByteArrayOutputStream

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import com.ldaniels528.trifecta.web.EmbeddedWebServer._
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
class EmbeddedWebServer() extends Logger {
  private val actorSystem = ActorSystem("EmbeddedWebServer", ConfigFactory.parseString(actorConfig))
  private var webServer: Option[WebServer] = None

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run() = EmbeddedWebServer.this.stop()
  })

  val routes = Routes({
    case GET(request) =>
      actorSystem.actorOf(Props[WebContentHandler]) ! request
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
  private val logger = LoggerFactory.getLogger(getClass)
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
   * Application entry point
   * @param args the given command line arguments
   */
  def main(args: Array[String]) {
    new EmbeddedWebServer().start()

    logger.info("Open your browser and navigate to http://localhost:8888")
  }

  /**
   * Web Content Handler
   * @author Lawrence Daniels <lawrence.daniels@gmail.com>
   */
  class WebContentHandler extends Actor {
    def receive = {
      case event: HttpRequestEvent =>
        val endPoint = event.request.endPoint
        val response = event.response
        val path = translatePath(endPoint.path)

        path match {
          case s => loadContent(s) map { bytes =>
            getMimeType(s) foreach (response.contentType = _)
            response.write(bytes)
          } getOrElse {
            logger.error(s"Resource '$path' not found")
            response.write(HttpResponseStatus.NOT_FOUND)
          }
        }
        context.stop(self)
    }

    /**
     * Returns the MIME type for the given resource
     * @param path the given resource path (e.g. "/images/greenLight.png")
     * @return the option of a MIME type (e.g. "image/png")
     */
    private def getMimeType(path: String): Option[String] = {
      path.lastIndexOptionOf(".") map (index => path.substring(index + 1)) flatMap {
        case "gif" => Some("image/gif")
        case "htm" | "html" => Some("text/html")
        case "jpg" | "jpeg" => Some("image/jpeg")
        case "js" => Some("text/javascript")
        case "png" => Some("image/png")
        case unknown =>
          logger.warn(s"No MIME type for $unknown")
          None
      }
    }

    /**
     * Retrieves the given resource from the class path
     * @param path the given resource path (e.g. "/images/greenLight.png")
     * @return the option of an array of bytes representing the content
     */
    private def loadContent(path: String): Option[Array[Byte]] = {
      Resource(path) map { url =>
        new ByteArrayOutputStream(1024) use { out =>
          url.openStream() use (IOUtils.copy(_, out))
          out.toByteArray
        }
      }
    }

    /**
     * Translates the given logical path to a physical path
     * @param path the given logical path
     * @return the corresponding physical path
     */
    private def translatePath(path: String) = path match {
      case "/" => "/app/index.htm"
      case s => s"/app$s"
    }

  }

}
