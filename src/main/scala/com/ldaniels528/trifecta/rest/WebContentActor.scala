package com.ldaniels528.trifecta.rest

import java.io.ByteArrayOutputStream

import akka.actor.Actor
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.WebContentActor._
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import net.liftweb.json._
import org.apache.commons.io.IOUtils
import org.mashupbots.socko.events.{HttpRequestEvent, HttpResponseStatus}
import org.slf4j.LoggerFactory

/**
 * Web Content Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebContentActor(facade: KafkaRestFacade) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)

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
        case "getQueries" if args.isEmpty => Option(facade.getQueries)
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
 * Web Content Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object WebContentActor {
  private val DocumentRoot = "/app"
  private val StreamingRoot = "/stream"
  private val RestRoot = "/rest"

}