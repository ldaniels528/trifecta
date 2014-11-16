package com.ldaniels528.trifecta.web

import akka.actor.{Actor, ActorSystem, Props}
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.web.EmbeddedWebServer._
import com.typesafe.config.ConfigFactory
import org.mashupbots.socko.events.{HttpRequestEvent, HttpResponseStatus}
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes.{GET, Routes}
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}

import scala.io.Source
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
      actorSystem.actorOf(Props[DummyHandler]) ! request
  })

  def start() {
    if (webServer.isEmpty) {
      webServer = Option(new WebServer(WebServerConfig(), routes, actorSystem))
      webServer.foreach(_.start())
    }
  }

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
  val actorConfig = """
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
   * Hello processor writes a greeting and stops.
   */
  class DummyHandler extends Actor {
    def receive = {
      case event: HttpRequestEvent =>
        val response = event.response
        Resource("/web/index.htm") map (Source.fromURL(_).getLines().mkString) match {
          case Some(content) =>
            response.contentType = "text/html"
            response.write(content)
          case None =>
            response.write(HttpResponseStatus.NOT_FOUND)
        }
        context.stop(self)
    }
  }

}


