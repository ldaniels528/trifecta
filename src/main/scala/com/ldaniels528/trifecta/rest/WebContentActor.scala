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

import scala.util.{Failure, Success, Try}

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
        processRestRequest(path, dataMap)

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

  private def processRestRequest(path: String, dataMap: Map[String, List[String]]): Option[(String, Array[Byte])] = {
    import java.net.URLDecoder.decode

    // get the action and path arguments
    val params = path.indexOptionOf(RestRoot)
      .map(index => path.substring(index + RestRoot.length + 1))
      .map(_.split("[/]")) map (a => (a.head, a.tail.toList))

    params foreach { case (action, args) => logger.info(s"processRestRequest Action: $action ~> values = $args")}
    dataMap foreach { case (key, values) => logger.info(s"processRestRequest Content: '$key' ~> values = $values")}

    // execute the action and get the JSON value
    params flatMap { case (action, args) =>
      action match {
        case "executeQuery" => facade.executeQuery(dataMap.getParam("queryString")).passJson
        case "findOne" => args match {
          case topic :: criteria :: Nil => facade.findOne(topic, decode(criteria, encoding)).passJson
          case _ => None
        }
        case "getConsumerDeltas" if args.isEmpty => JsonHelper.toJson(facade.getConsumerDeltas).passJson
        case "getConsumers" if args.isEmpty => facade.getConsumers.passJson
        case "getConsumerSet" if args.isEmpty => facade.getConsumerSet.passJson
        case "getMessage" => args match {
          case topic :: partition :: offset :: Nil => facade.getMessage(topic, partition.toInt, offset.toLong).passJson
          case _ => None
        }
        case "getQueries" if args.isEmpty => facade.getQueries.passJson
        case "getTopicByName" => args match {
          case name :: Nil => facade.getTopicByName(name).passJson
          case _ => None
        }
        case "getTopicDeltas" if args.isEmpty => JsonHelper.toJson(facade.getTopicDeltas).passJson
        case "getTopicDetailsByName" => args match {
          case name :: Nil => facade.getTopicDetailsByName(name).passJson
          case _ => None
        }
        case "getTopics" if args.isEmpty => facade.getTopics.passJson
        case "getTopicSummaries" if args.isEmpty => facade.getTopicSummaries.passJson
        case "getZkData" => facade.getZkData(toZkPath(args.init), args.last).passJson
        case "getZkInfo" => facade.getZkInfo(toZkPath(args)).passJson
        case "getZkPath" => facade.getZkPath(toZkPath(args)).passJson
        case "saveQuery" if dataMap.nonEmpty =>
          val name = dataMap.getParam("name")
          val queryString = dataMap.getParam("queryString")
          facade.saveQuery(name, queryString).passJson
        case "transformResultsToCSV" if dataMap.nonEmpty =>
          facade.transformResultsToCSV(dataMap.getParam("queryResults")).passCsv
        case _ => None
      }
    }
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
  private val RestRoot = "/rest"
  private val encoding = "UTF8"

  private val MimeCsv = "text/csv"
  private val MimeJson = "application/json"

  /**
   * Convenience method for returning a parameter from a data map
   * @param dataMap the given data map
   */
  implicit class DataMapHelper(val dataMap: Map[String, List[String]]) extends AnyVal {

    def getParam(name: String): Option[String] = dataMap.get(name) flatMap (_.headOption)

  }

  /**
   * Convenience method for returning an option of a JSON mime type and associated binary content
   * @param json the option of a JSON Value
   */
  implicit class JsonExtensionsA(val json: Option[JValue]) extends AnyVal {

    def passJson: Option[(String, Array[Byte])] = json map (js => (MimeJson, compact(render(js)).getBytes(encoding)))

  }

  /**
   * Convenience method for returning an option of a JSON mime type and associated binary content
   * @param json a JSON Value
   */
  implicit class JsonExtensionsB(val json: JValue) extends AnyVal {

    def passJson: Option[(String, Array[Byte])] = Option(json) map (js => (MimeJson, compact(render(js)).getBytes(encoding)))

  }

  implicit class TryExtensions(val outcome: Try[Option[List[String]]]) extends AnyVal {

    def passCsv: Option[(String, Array[Byte])] = {
      outcome match {
        case Success(Some(list)) => Some((MimeCsv, list.mkString("\n").getBytes(encoding)))
        case Success(None) => Some((MimeCsv, Array.empty[Byte]))
        case Failure(e) =>
          throw new IllegalStateException("Error processing request", e)
      }
    }

  }

}