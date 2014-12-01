package com.ldaniels528.trifecta.rest

import java.text.SimpleDateFormat
import java.util.GregorianCalendar

import akka.actor.{Actor, ActorSystem, Props, actorRef2Scala}
import akka.event.Logging
import org.mashupbots.socko.events.{HttpRequestEvent, HttpResponseStatus, WebSocketFrameEvent}
import org.mashupbots.socko.infrastructure.Logger
import org.mashupbots.socko.routes._
import org.mashupbots.socko.webserver.{WebServer, WebServerConfig}

/**
 * This example shows how to use web sockets, specifically `org.mashupbots.socko.processors.WebSocketBroadcaster`,
 * for chatting.
 *
 * With `org.mashupbots.socko.processors.WebSocketBroadcaster`, you can broadcast messages to all registered web
 * socket connections
 *
 * - Open a few browsers and navigate to `http://localhost:8888/html`.
 * - A HTML page will be displayed
 * - It will make a web socket connection to `ws://localhost:8888/websocket/`
 * - Type in some text on one browser and see it come up on the other browsers
 */
object ChatApp extends Logger {
  //
  // STEP #1 - Define Actors and Start Akka
  // `ChatHandler` is created in the route and is self-terminating
  //
  val actorSystem = ActorSystem("ChatExampleActorSystem")

  //
  // STEP #2 - Define Routes
  // Each route dispatches the request to a newly instanced `WebSocketHandler` actor for processing.
  // `WebSocketHandler` will `stop()` itself after processing the request.
  //
  val routes = Routes({

    case HttpRequest(httpRequest) => httpRequest match {
      case GET(Path("/html")) => {
        // Return HTML page to establish web socket
        actorSystem.actorOf(Props[ChatHandler]) ! httpRequest
      }
      case Path("/favicon.ico") => {
        // If favicon.ico, just return a 404 because we don't have that file
        httpRequest.response.write(HttpResponseStatus.NOT_FOUND)
      }
    }

    case WebSocketHandshake(wsHandshake) => wsHandshake match {
      case Path("/websocket/") => {
        // To start Web Socket processing, we first have to authorize the handshake.
        // This is a security measure to make sure that web sockets can only be established at your specified end points.
        wsHandshake.authorize(
          onComplete = Some(onWebSocketHandshakeComplete),
          onClose = Some(onWebSocketClose))
      }
    }

    case WebSocketFrame(wsFrame) => {
      // Once handshaking has taken place, we can now process frames sent from the client
      actorSystem.actorOf(Props[ChatHandler]) ! wsFrame
    }

  })

  val webServer = new WebServer(WebServerConfig(), routes, actorSystem)

  //
  // STEP #3 - Start and Stop Socko Web Server
  //
  def main(args: Array[String]) {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run {
        webServer.stop()
      }
    })
    webServer.start()

    System.out.println("Open a few browsers and navigate to http://localhost:8888/html. Start chatting!")
  }

  def onWebSocketHandshakeComplete(webSocketId: String) {
    System.out.println(s"Web Socket $webSocketId connected")
  }

  def onWebSocketClose(webSocketId: String) {
    System.out.println(s"Web Socket $webSocketId closed")
  }

}

/**
 * Web Socket processor for chatting
 */
class ChatHandler extends Actor {
  val log = Logging(context.system, this)

  /**
   * Process incoming events
   */
  def receive = {
    case event: HttpRequestEvent =>
      // Return the HTML page to setup web sockets in the browser
      writeHTML(event)
      context.stop(self)
    case event: WebSocketFrameEvent =>
      // Echo web socket text frames
      writeWebSocketResponse(event)
      context.stop(self)
    case _ => {
      log.info("received unknown message of type: ")
      context.stop(self)
    }
  }

  /**
   * Write HTML page to setup a web socket on the browser
   */
  private def writeHTML(ctx: HttpRequestEvent) {
    // Send 100 continue if required
    if (ctx.request.is100ContinueExpected) {
      ctx.response.write100Continue()
    }

    val buf = new StringBuilder()
    buf.append("<html><head><title>Socko Web Socket Example</title></head>\n")
    buf.append("<body>\n")
    buf.append("<script type=\"text/javascript\">\n")
    buf.append("  var socket;\n")
    buf.append("  if (!window.WebSocket) {\n")
    buf.append("    window.WebSocket = window.MozWebSocket;\n")
    buf.append("  }\n")
    buf.append("  if (window.WebSocket) {\n")
    buf.append("    socket = new WebSocket(\"ws://localhost:8888/websocket/\");\n") // Note the address must match the route
    buf.append("    socket.onmessage = function(event) { var ta = document.getElementById('responseText'); ta.value = ta.value + '\\n' + event.data };\n")
    buf.append("    socket.onopen = function(event) { var ta = document.getElementById('responseText'); ta.value = \"Web Socket opened!\"; };\n")
    buf.append("    socket.onclose = function(event) { var ta = document.getElementById('responseText'); ta.value = ta.value + \"Web Socket closed\"; };\n")
    buf.append("  } else { \n")
    buf.append("    alert(\"Your browser does not support Web Sockets.\");\n")
    buf.append("  }\n")
    buf.append("  \n")
    buf.append("  function send(message) {\n")
    buf.append("    if (!window.WebSocket) { return; }\n")
    buf.append("    if (socket.readyState == WebSocket.OPEN) {\n")
    buf.append("      socket.send(message);\n")
    buf.append("    } else {\n")
    buf.append("      alert(\"The socket is not open.\");\n")
    buf.append("    }\n")
    buf.append("  }\n")
    buf.append("</script>\n")
    buf.append("<h1>Socko Web Socket Chat Example</h1>\n")
    buf.append("<form onsubmit=\"return false;\">\n")
    buf.append("  <input type=\"text\" name=\"message\" value=\"Hello, World!\"/>\n")
    buf.append("  <input type=\"button\" value=\"Chat\" onclick=\"send(this.form.message.value)\" />\n")
    buf.append("  \n")
    buf.append("  <h3>Output</h3>\n")
    buf.append("  <textarea id=\"responseText\" style=\"width: 500px; height:300px;\"></textarea>\n")
    buf.append("</form>\n")
    buf.append("</body>\n")
    buf.append("</html>\n")

    ctx.response.write(buf.toString, "text/html; charset=UTF-8")
  }

  /**
   * Echo the details of the web socket frame that we just received; but in upper case.
   */
  private def writeWebSocketResponse(event: WebSocketFrameEvent) {
    log.info("TextWebSocketFrame: " + event.readText)

    val dateFormatter = new SimpleDateFormat("HH:mm:ss")
    val time = new GregorianCalendar()
    val ts = dateFormatter.format(time.getTime())

    ChatApp.webServer.webSocketConnections.writeText(ts + " " + event.readText)
  }

}