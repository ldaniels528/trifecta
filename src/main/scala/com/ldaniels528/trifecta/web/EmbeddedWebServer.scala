package com.ldaniels528.trifecta.web

import org.mortbay.jetty._
import org.mortbay.jetty.handler._
import org.mortbay.jetty.nio.SelectChannelConnector
import org.mortbay.thread.QueuedThreadPool

/**
 * Embedded Web Server
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class EmbeddedWebServer(port: Int) {
  val connector = new SelectChannelConnector()
  connector.setHost("127.0.0.1")
  connector.setPort(8888)
  connector.setThreadPool(new QueuedThreadPool(20))
  connector.setName("admin")

  val resourceHandler = new ResourceHandler()
  resourceHandler.setWelcomeFiles(Array[String]("index.html"))
  resourceHandler.setResourceBase(".")

  val handlers = new HandlerList()
  handlers.setHandlers(Array[Handler](resourceHandler, new DefaultHandler()))

  val server = new Server()
  server.addConnector(connector)
  server.setHandler(handlers)
  server.start()
  server.join()

  def start(): Unit = {

  }

  def stop(): Unit = {

  }

}
