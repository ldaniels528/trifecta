import play.api.mvc.Results._
import play.api.mvc.{Filter, RequestHeader, Result, WithFilters}
import play.api.{Application, GlobalSettings, Logger}
import play.filters.gzip.GzipFilter

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

/**
 * Global Settings
 * @author lawrence.daniels@gmail.com
 */
object Global extends WithFilters(LoggingFilter, new GzipFilter()) with GlobalSettings {

  override def onStart(app: Application) = Logger.info("Application has started")

  override def onStop(app: Application) = Logger.info("Application shutdown...")

  override def onBadRequest(request: RequestHeader, error: String) = Future.successful(BadRequest(s"Bad Request: $error"))

  //override def onHandlerNotFound(request: RequestHeader)=  NotFound(views.html.notFoundPage(request.path))

  //override def onError(request: RequestHeader, t: Throwable) = InternalServerError(views.html.errorPage(t))

}

/**
 * Logging Filter
 * @author lawrence.daniels@gmail.com
 *         http://www.playframework.com/documentation/2.2.3/ScalaHttpFilters
 */
object LoggingFilter extends Filter {

  def apply(nextFilter: (RequestHeader) => Future[Result])(requestHeader: RequestHeader): Future[Result] = {
    val startTime = System.nanoTime
    nextFilter(requestHeader).map { result =>
      val endTime = System.nanoTime
      val requestTime = (endTime - startTime) / 1e+6f
      if (requestHeader.uri.startsWith("/api")) {
        Logger.info(f"${requestHeader.method} ${requestHeader.uri} ~> ${result.header.status} [$requestTime%.1f ms]")
      }
      result.withHeaders("Request-Time" -> requestTime.toString)
    }
  }
}