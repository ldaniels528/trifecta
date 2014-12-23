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

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Web Content Actor
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class WebContentActor(facade: KafkaRestFacade)(implicit ec: ExecutionContext) extends Actor {
  private lazy val logger = LoggerFactory.getLogger(getClass)

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

      def verb(path: String) = if(path.startsWith(RestRoot)) "Executed" else "Retrieved"

      getContent(path, request) match {
        // is it the option of a result?
        case Left(result_?) =>
          result_? map { case (mimeType, bytes) =>
            val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
            logger.info(f"SYNC: ${verb(path)} '$path' (${bytes.length} bytes) [$elapsedTime%.1f msec]")
            response.contentType = mimeType
            response.write(bytes)
          } getOrElse {
            logger.error(s"Resource '$path' not found")
            response.write(HttpResponseStatus.NOT_FOUND, s"Resource '$path' not found")
          }

        // Or is it a promise of a result?
        case Right(outcome) => outcome.onComplete {
          case Success((mimeType, bytes)) =>
            val elapsedTime = (System.nanoTime() - startTime).toDouble / 1e+6
            logger.info(f"ASYNC: Retrieved '$path' (${bytes.length} bytes) [$elapsedTime%.1f msec]")
            response.contentType = mimeType
            response.write(bytes)
          case Failure(e) =>
            logger.error(s"Resource '$path' failed during processing", e)
            response.write(HttpResponseStatus.INTERNAL_SERVER_ERROR, s"Resource '$path' failed during processing")
        }
      }
  }

  /**
   * Retrieves the given resource from the class path
   * @param path the given resource path (e.g. "/images/greenLight.png")
   * @return the option of an array of bytes representing the content
   */
  private def getContent(path: String, request: CurrentHttpRequestMessage): Either[Option[(String, Array[Byte])], Future[(String, Array[Byte])]] = {
    path match {
      // REST requests
      case s if s.startsWith(RestRoot) =>
        processRestRequest(path, request)

      // resource requests
      case s =>
        Left {
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

  private def processRestRequest(path: String, request: CurrentHttpRequestMessage): Either[Option[(String, Array[Byte])], Future[(String, Array[Byte])]] = {
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
    Try {
      params match {
        case None => throw new RuntimeException(s"Invalid request [$path]")
        case Some((action, args)) =>
          action match {
            case "executeQuery" => Right(facade.executeQuery(request.asJsonString).toJson.mimeJson)
            case "findOne" => args match {
              case topic :: criteria :: Nil => Right(facade.findOne(topic, decode(criteria, encoding)).toJson.mimeJson)
              case _ => missingArgs("topic", "criteria")
            }
            case "getBrokers" => Left(facade.getBrokers.toJson.mimeJson)
            case "getConsumerDeltas" => Left(facade.getConsumerDeltas.toJson.mimeJson)
            case "getConsumers" => Left(facade.getConsumers.toJson.mimeJson)
            case "getConsumerSet" => Left(facade.getConsumerSet.toJson.mimeJson)
            case "getDecoders" => Left(facade.getDecoders.toJson.mimeJson)
            case "getDecoderByTopic" => args match {
              case topic :: Nil => Left(facade.getDecoderByTopic(topic).toJson.mimeJson)
              case _ => missingArgs("topic")
            }
            case "getDecoderSchemaByName" => args match {
              case topic :: schemaName :: Nil => Left(facade.getDecoderSchemaByName(topic, schemaName).toJson.mimeJson)
              case _ => missingArgs("topic", "schemaName")
            }
            case "getLeaderAndReplicas" => args match {
              case topic :: Nil => Left(facade.getLeaderAndReplicas(topic).toJson.mimeJson)
              case _ => missingArgs("topic")
            }
            case "getMessage" => args match {
              case topic :: partition :: offset :: Nil => Left(facade.getMessageData(topic, partition.toInt, offset.toLong).toJson.mimeJson)
              case _ => missingArgs("topic", "partition", "offset")
            }
            case "getMessageKey" => args match {
              case topic :: partition :: offset :: Nil => Left(facade.getMessageKey(topic, partition.toInt, offset.toLong).toJson.mimeJson)
              case _ => missingArgs("topic", "partition", "offset")
            }
            case "getQueriesByTopic" => args match {
              case topic :: Nil => Left(facade.getQueriesByTopic(topic).toJson.mimeJson)
              case _ => missingArgs("topic")
            }
            case "getReplicas" => args match {
              case topic :: Nil => Left(facade.getReplicas(topic).toJson.mimeJson)
              case _ => missingArgs("topic")
            }
            case "getTopicByName" => args match {
              case name :: Nil => Left(facade.getTopicByName(name).toJson.mimeJson)
              case _ => missingArgs("name")
            }
            case "getTopicDeltas" => Left(JsonHelper.toJson(facade.getTopicDeltas).mimeJson)
            case "getTopicDetailsByName" => args match {
              case name :: Nil => Left(facade.getTopicDetailsByName(name).toJson.mimeJson)
              case _ => missingArgs("name")
            }
            case "getTopics" => Left(facade.getTopics.toJson.mimeJson)
            case "getTopicSummaries" => Left(facade.getTopicSummaries.toJson.mimeJson)
            case "getZkData" => Left(facade.getZkData(toZkPath(args.init), args.last).toJson.mimeJson)
            case "getZkInfo" => Left(facade.getZkInfo(toZkPath(args)).toJson.mimeJson)
            case "getZkPath" => Left(facade.getZkPath(toZkPath(args)).toJson.mimeJson)
            case "publishMessage" => args match {
              case topic :: Nil => Left(facade.publishMessage(topic, request.asJsonString).toJson.mimeJson)
              case _ => missingArgs("topic")
            }
            case "saveQuery" => Left(facade.saveQuery(request.asJsonString).toJson.mimeJson)
            case "saveSchema" => Left(facade.saveDecoderSchema(request.asJsonString).toJson.mimeJson)
            case "transformResultsToCSV" => Left(facade.transformResultsToCSV(request.asJsonString).mimeCSV)
            case _ => Left(None)
          }
      }
    } match {
      case Success(content) => content
      case Failure(e) =>
        logger.error(s"Error processing $path", e)
        Left(JsonHelper.decompose(KafkaRestFacade.ErrorJs(message = e.getMessage)).mimeJson)
    }
  }

  private def missingArgs[S](argNames: String*): S = {
    throw new RuntimeException(s"Expected arguments (${argNames mkString ", "}) are missing")
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
   * @param json a JSON Value
   */
  implicit class JsonExtensionsA(val json: JValue) extends AnyVal {

    def mime(mimeType: String): Option[(String, Array[Byte])] = Option(json) map (js => (mimeType, compact(render(js)).getBytes(encoding)))

    def mimeJson: Option[(String, Array[Byte])] = Option(json) map (js => (MimeJson, compact(render(js)).getBytes(encoding)))

  }

  /**
   * Convenience method for returning an option of a JSON mime type and associated binary content
   * @param json the option of a JSON Value
   */
  implicit class JsonExtensionsB(val json: Option[JValue]) extends AnyVal {

    def mimeJson: Option[(String, Array[Byte])] = json map (js => (MimeJson, compact(render(js)).getBytes(encoding)))

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

    def mimeJson(implicit ec: ExecutionContext): Future[(String, Array[Byte])] = outcome map (js => (MimeJson, compact(render(js)).getBytes(encoding)))

  }

  /**
   * Convenience method for returning the corresponding JSON value
   * @param values the collection of values
   */
  implicit class SeqJsonExtension[T](val values: Seq[T]) extends AnyVal {

    def toJson: JValue = JsonHelper.decompose(values)

  }


  implicit class TryJsonExtension[T](val outcome: Try[T]) extends AnyVal {

    def toJson: JValue = JsonHelper.decompose(
      outcome match {
        case Success(v) => v
        case Failure(e) => KafkaRestFacade.ErrorJs(message = e.getMessage)
      })

  }

  implicit class TryExtensions(val outcome: Try[Option[List[String]]]) extends AnyVal {

    def mimeCSV: Option[(String, Array[Byte])] = {
      outcome match {
        case Success(Some(list)) => Some((MimeCsv, list.mkString("\n").getBytes(encoding)))
        case Success(None) => Some((MimeCsv, Array.empty[Byte]))
        case Failure(e) =>
          throw new IllegalStateException("Error processing request", e)
      }
    }

  }

}