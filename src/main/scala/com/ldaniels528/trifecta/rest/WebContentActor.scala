package com.ldaniels528.trifecta.rest

import java.io.ByteArrayOutputStream

import akka.actor.{ActorLogging, Actor}
import com.ldaniels528.trifecta.io.json.JsonHelper
import com.ldaniels528.trifecta.rest.WebContentActor._
import com.ldaniels528.commons.helpers.OptionHelper._
import com.ldaniels528.commons.helpers.Resource
import com.ldaniels528.commons.helpers.ResourceHelper._
import com.ldaniels528.commons.helpers.StringHelper._
import net.liftweb.json._
import org.apache.commons.io.IOUtils
import org.mashupbots.socko.events.{CurrentHttpRequestMessage, HttpRequestEvent, HttpResponseStatus}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Try, Failure, Success}

/**
 * Web Content Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebContentActor(facade: KafkaRestFacade) extends Actor with ActorLogging {
  import context.dispatcher

  override def receive = {
    case event: HttpRequestEvent =>
      val startTime = System.nanoTime()
      val request = event.request
      val response = event.response
      val path = translatePath(request.endPoint.path)

      // send 100 continue if required
      if (event.request.is100ContinueExpected) {
        event.response.write100Continue()
      }

      getContent(path, request) onComplete {
        case Success(Content(mimeType, bytes)) =>
          val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
          log.info(f"${verb(path)} '$path' (${bytes.length} bytes) [$elapsedTime%.1f msec]")
          response.contentType = mimeType
          response.write(bytes)
        case Failure(e) =>
          val message = s"Resource '$path' failed unexpectedly"
          log.error(message, e)
          response.write(statusCode(path), message)
      }
  }

  /**
   * Retrieves the given resource from the class path
   * @param resourcePath the given resource path (e.g. "/images/greenLight.png")
   * @return the option of an array of bytes representing the content
   */
  private def getContent(resourcePath: String, request: CurrentHttpRequestMessage): Future[Content] = {
    resourcePath match {
      case path if path.startsWith(RestRoot) => processRestRequest(path, request)
      case path => handleResourceRequest(path)
    }
  }

  /**
   * Asynchronously handles the GET request for the given resource path
   * @param path the given resource path
   * @return a promise of a piece of content
   */
  private def handleResourceRequest(path: String) = Future {
    (for {
      bytes <- Resource(path) map (url =>
        new ByteArrayOutputStream(1024) use { out =>
          url.openStream() use (IOUtils.copy(_, out))
          out.toByteArray
        })
      mimeType <- guessMimeType(path)
    } yield Content(mimeType, bytes)).orDie(s"Resource '$path' not found")
  }

  private def verb(path: String) = if (path.startsWith(RestRoot)) "Executed" else "Retrieved"

  private def statusCode(path: String) = if (path.startsWith(RestRoot)) HttpResponseStatus.INTERNAL_SERVER_ERROR else HttpResponseStatus.NOT_FOUND

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
        log.warning(s"No MIME type found for '$path'")
        None
    }
  }

  private def processRestRequest(path: String, request: CurrentHttpRequestMessage): Future[Content] = {
    import java.net.URLDecoder.decode

    /**
     * Returns the form parameters as a data mapping
     * @return a [[Map]] of a key to a [[List]] of string values
     */
    def form = request.content.toFormDataMap

    /**
     * Returns the parameters from the REST url
     * @return the parameters
     */
    def params: Option[(String, List[String])] = {
      path.indexOptionOf(RestRoot)
        .map(index => path.substring(index + RestRoot.length + 1))
        .map(_.split("[/]")) map (a => (a.head, a.tail.toList))
    }

    // execute the action and get the JSON value
    params match {
      case None => throw new RuntimeException(s"Invalid request [$path]")
      case Some((action, args)) =>
        action match {
          case "executeQuery" => facade.executeQuery(request.asJsonString).toJson.mimeJson
          case "findOne" => args match {
            case topic :: criteria :: Nil => facade.findOne(topic, decode(criteria, encoding)).toJson.mimeJson
            case _ => missingArgs("topic", "criteria")
          }
          case "getBrokers" => facade.getBrokers.toJson.mimeJson
          case "getBrokerDetails" => facade.getBrokerDetails.toJson.mimeJson
          case "getConsumerDeltas" => facade.getConsumerDeltas.toJson.mimeJson
          case "getConsumerDetails" => facade.getConsumerDetails.toJson.mimeJson
          case "getConsumersByTopic" => args match {
            case topic :: Nil => facade.getConsumersByTopic(topic).toJson.mimeJson
            case _ => missingArgs("topic")
          }
          case "getDecoders" => facade.getDecoders.toJson.mimeJson
          case "getDecoderByTopic" => args match {
            case topic :: Nil => facade.getDecoderByTopic(topic).toJson.mimeJson
            case _ => missingArgs("topic")
          }
          case "getDecoderSchemaByName" => args match {
            case topic :: schemaName :: Nil => facade.getDecoderSchemaByName(topic, schemaName).toJson.mimeJson
            case _ => missingArgs("topic", "schemaName")
          }
          case "getMessage" => args match {
            case topic :: partition :: offset :: Nil => facade.getMessageData(topic, partition.toInt, offset.toLong).toJson.mimeJson
            case _ => missingArgs("topic", "partition", "offset")
          }
          case "getMessageKey" => args match {
            case topic :: partition :: offset :: Nil => facade.getMessageKey(topic, partition.toInt, offset.toLong).toJson.mimeJson
            case _ => missingArgs("topic", "partition", "offset")
          }
          case "getQueriesByTopic" => args match {
            case topic :: Nil => facade.getQueriesByTopic(topic).toJson.mimeJson
            case _ => missingArgs("topic")
          }
          case "getReplicas" => args match {
            case topic :: Nil => facade.getReplicas(topic).toJson.mimeJson
            case _ => missingArgs("topic")
          }
          case "getTopicByName" => args match {
            case name :: Nil => facade.getTopicByName(name).toJson.mimeJson
            case _ => missingArgs("name")
          }
          case "getTopicDeltas" => facade.getTopicDeltas.toJson.mimeJson
          case "getTopicDetailsByName" => args match {
            case name :: Nil => facade.getTopicDetailsByName(name).toJson.mimeJson
            case _ => missingArgs("name")
          }
          case "getTopics" => facade.getTopics.toJson.mimeJson
          case "getTopicSummaries" => facade.getTopicSummaries.toJson.mimeJson
          case "getZkData" => facade.getZkData(toZkPath(args.init), args.last).toJson.mimeJson
          case "getZkInfo" => facade.getZkInfo(toZkPath(args)).toJson.mimeJson
          case "getZkPath" => facade.getZkPath(toZkPath(args)).toJson.mimeJson
          case "publishMessage" => args match {
            case topic :: Nil => facade.publishMessage(topic, request.asJsonString).toJson.mimeJson
            case _ => missingArgs("topic")
          }
          case "saveQuery" => facade.saveQuery(request.asJsonString).toJson.mimeJson
          case "saveSchema" => facade.saveDecoderSchema(request.asJsonString).toJson.mimeJson
          case "transformResultsToCSV" => facade.transformResultsToCSV(request.asJsonString).mimeCSV
          case unknown => Future.failed(new RuntimeException(s"Action not found '$unknown'"))
        }
    }
  }

  private def missingArgs(argNames: String*) = {
    Future.failed(new RuntimeException(s"Expected arguments (${argNames mkString ", "}) are missing"))
  }

  private def toZkPath(args: List[String]): String = "/" + args.mkString("/")

  /**
   * Translates the given logical path to a physical path
   * @param path the given logical path
   * @return the corresponding physical path
   */
  private def translatePath(path: String) = path match {
    case "/" | "/index.htm" | "/index.html" => s"$DocumentRoot/index.htm"
    case "/favicon.ico" => s"$DocumentRoot/favicon.ico"
    case s => s
  }

}

/**
 * Web Content Actor Singleton
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
object WebContentActor {
  private[this] lazy val logger = LoggerFactory.getLogger(getClass)
  private val DocumentRoot = "/app"
  private val RestRoot = "/rest"
  private val encoding = "UTF8"

  private val MimeCsv = "text/csv"
  private val MimeJson = "application/json"

  /**
   * Represents a "piece" of web content
   * @param mimeType the given MIME type of the content
   * @param bytes the binary representation of the content
   */
  case class Content(mimeType: String, bytes: Array[Byte])

  /**
   * Convenience method for returning a parameter from a data map
   * @param dataMap the given data map
   */
  implicit class DataMapHelper(val dataMap: Map[String, List[String]]) extends AnyVal {

    def getParam(name: String): Option[String] = dataMap.get(name) flatMap (_.headOption)

    def getParamOrDie(name: String): String = dataMap.get(name) flatMap (_.headOption) orDie s"Parameter '$name' is required"

  }

  /**
   * Convenience method for returning the corresponding JSON value
   * @param outcome the promise of a value
   */
  implicit class FutureJsonExtensionA[T](val outcome: Future[T]) extends AnyVal {

    def toJson(implicit ec: ExecutionContext): Future[JValue] = outcome.map(JsonHelper.decompose(_))

  }

  /**
   * Convenience method for returning an option of a JSON mime type and associated binary content
   * @param outcome the promise of a value
   */
  implicit class FutureJsonExtensionB[T](val outcome: Future[JValue]) extends AnyVal {

    def mimeJson(implicit ec: ExecutionContext): Future[Content] = {
      outcome map (js => Content(MimeJson, express(js)))
    }

    private def express(js: JValue): Array[Byte] = {
      if(js == JNothing) Array.empty
      else {
        Try(compact(render(js)).getBytes(encoding)) match {
          case Success(bytes) => bytes
          case Failure(e) =>
            logger.error(s"Unexpected error rendering: $js", e)
            Array.empty
        }
      }
    }

  }

  implicit class FutureJsonExtensionsC(val outcome: Future[Option[List[String]]]) extends AnyVal {

    def mimeCSV(implicit ec: ExecutionContext): Future[Content] = {
      outcome map {
        case Some(list) => Content(MimeCsv, list.mkString("\n").getBytes(encoding))
        case None => Content(MimeCsv, Array.empty[Byte])
      }
    }

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

}