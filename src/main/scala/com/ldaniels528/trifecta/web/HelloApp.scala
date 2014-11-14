package com.ldaniels528.trifecta.web

import org.mashupbots.socko.infrastructure.Logger

object HelloApp extends Logger {

  //
  // STEP #3 - Start and Stop Socko Web Server
  //
  def main(args: Array[String]) {
    val webServer = new EmbeddedWebServer()
    webServer.start()

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run() = webServer.stop()
    })

    System.out.println("Open your browser and navigate to http://localhost:8888")
  }
}
