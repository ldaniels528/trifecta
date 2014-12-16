package com.ldaniels528.trifecta.rest

import java.io.ByteArrayOutputStream

import akka.actor.Actor
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.WebContentActor._
import com.ldaniels528.trifecta.util.OptionHelper._
import com.ldaniels528.trifecta.util.Resource
import com.ldaniels528.trifecta.util.ResourceHelper._
import com.ldaniels528.trifecta.util.StringHelper._
import net.liftweb.json._
import org.apache.commons.io.IOUtils
import org.mashupbots.socko.events.{CurrentHttpRequestMessage, HttpRequestEvent, HttpResponseStatus}
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

      getContent(path, request) map { case (mimeType, bytes) =>
        val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
        logger.info(f"Retrieved resource '$path' (${bytes.length} bytes) [$elapsedTime%.1f msec]")
        response.contentType = mimeType
        response.write(bytes)
      } getOrElse {
        logger.error(s"Resource '$path' not found")
        response.write(HttpResponseStatus.NOT_FOUND, s"Resource '$path' not found")
      }
  }

  /**
   * Retrieves the given resource from the class path
   * @param path the given resource path (e.g. "/images/greenLight.png")
   * @return the option of an array of bytes representing the content
   */
  private def getContent(path: String, request: CurrentHttpRequestMessage): Option[(String, Array[Byte])] = {
    path match {
      // REST requests
      case s if s.startsWith(RestRoot) =>
        processRestRequest(path, request)

      // resource requests
      case s =>
        for {
          bytes <- Resource(s) map (url =>
            new ByteArrayOutputStream(1024) use { out =>
              url.openStream() use (IOUtils.copy(_, out))
              out.toByteArray
            })
          mimeType <- guessMimeType(s)
        } yield mimeType -> bytes
    }
  }

  /**
   * Returns the MIME type for the given resource
   * @param path the given resource path (e.g. "/images/greenLight.png")
   * @return the option of a MIME type (e.g. "image/png")
   */
  private def guessMimeType(path: String): Option[String] = {
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

  private def processRestRequest(path: String, request: CurrentHttpRequestMessage): Option[(String, Array[Byte])] = {
    import java.net.URLDecoder.decode
    implicit val formats = net.liftweb.json.DefaultFormats

    /**
     * Returns the form parameters as a data mapping
     * @return a data mapping
     */
    def form = {
      val map = request.content.toFormDataMap
      map foreach { case (key, values) => logger.info(s"processRestRequest Content: '$key' ~> values = $values")}
      map
    }

    /**
     * Returns the parameters from the REST url
     * @return the parameters
     */
    def params = {
      val p = path.indexOptionOf(RestRoot)
        .map(index => path.substring(index + RestRoot.length + 1))
        .map(_.split("[/]")) map (a => (a.head, a.tail.toList))
      p foreach { case (action, args) => logger.info(s"processRestRequest Action: $action ~> values = $args")}
      p
    }

    // execute the action and get the JSON value
    Try {
      params flatMap { case (action, args) =>
        action match {
          case "executeQuery" => facade.executeQuery(request.asJsonString).passJson
          case "findOne" => args match {
            case topic :: criteria :: Nil => facade.findOne(topic, decode(criteria, encoding)).passJson
            case _ => missingArgs("topic", "criteria")
          }
          case "getConsumerDeltas" => JsonHelper.toJson(facade.getConsumerDeltas).passJson
          case "getConsumers" => facade.getConsumers.passJson
          case "getConsumerSet" => facade.getConsumerSet.passJson
          case "getDecoders" => facade.getDecoders.passJson
          case "getDecoderByTopic" => args match {
            case topic :: Nil => facade.getDecoderByTopic(topic).passJson
            case _ => missingArgs("topic")
          }
          case "getLeaderAndReplicas" => args match {
            case topic :: Nil => facade.getLeaderAndReplicas(topic).passJson
            case _ => missingArgs("topic")
          }
          case "getMessage" => args match {
            case topic :: partition :: offset :: Nil => facade.getMessageData(topic, partition.toInt, offset.toLong).passJson
            case _ => missingArgs("topic", "partition", "offset")
          }
          case "getMessageKey" => args match {
            case topic :: partition :: offset :: Nil => facade.getMessageKey(topic, partition.toInt, offset.toLong).passJson
            case _ => missingArgs("topic", "partition", "offset")
          }
          case "getQueries" => facade.getQueries.passJson
          case "getReplicas" => args match {
            case topic :: Nil => facade.getReplicas(topic).passJson
            case _ => missingArgs("topic")
          }
          case "getTopicByName" => args match {
            case name :: Nil => facade.getTopicByName(name).passJson
            case _ => missingArgs("name")
          }
          case "getTopicDeltas" => JsonHelper.toJson(facade.getTopicDeltas).passJson
          case "getTopicDetailsByName" => args match {
            case name :: Nil => facade.getTopicDetailsByName(name).passJson
            case _ => missingArgs("name")
          }
          case "getTopics" => facade.getTopics.passJson
          case "getTopicSummaries" => facade.getTopicSummaries.passJson
          case "getZkData" => facade.getZkData(toZkPath(args.init), args.last).passJson
          case "getZkInfo" => facade.getZkInfo(toZkPath(args)).passJson
          case "getZkPath" => facade.getZkPath(toZkPath(args)).passJson
          case "publishMessage" => args match {
            case topic :: Nil => facade.publishMessage(topic, request.asJsonString).passJson
            case _ => missingArgs("topic")
          }
          case "saveQuery" => facade.saveQuery(request.asJsonString).passJson
          case "saveSchema" => facade.saveSchema(request.asJsonString).passJson
          case "transformResultsToCSV" => facade.transformResultsToCSV(request.asJsonString).passCsv
          case _ => None
        }
      }
    } match {
      case Success(v) => v
      case Failure(e) =>
        logger.error(s"Error processing $path", e)
        Extraction.decompose(KafkaRestFacade.ErrorJs(message = e.getMessage)).passJson
    }
  }

  private def missingArgs[S](argNames: String*): S = {
    throw new IllegalStateException(s"Expected arguments (${argNames mkString ", "}) are missing")
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

    def getParamOrDie(name: String): String = dataMap.get(name) flatMap (_.headOption) orDie s"Parameter '$name' is required"

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

  /**
   * HTTP Request Extensions
   * @param request the given [[CurrentHttpRequestMessage]]
   */
  implicit class RequestExtension(val request: CurrentHttpRequestMessage) extends AnyVal {

    /**
     * Returns the content as a JSON string
     * @return a JSON string
     */
    def asJsonString = new String(request.content.toBytes)

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