package com.ldaniels528.verify.util

/**
 * Verify JSON Customized Parser
 * @author lawrence.daniels@gmail.com
 */
object VerifyJson {
  import com.google.gson._
  import java.lang.reflect.Type
  import java.text.SimpleDateFormat
  import java.util.Date
  import scala.util.Try

  val DATE_FORMAT1 = "yyyy-MM-dd HH:mm:ss" // e.g. '2014-03-12 19:10:13'
  val DATE_FORMAT2 = "MMM dd',' yyyy hh:mm:ss a"

  def getJsonParser: Gson = {
    val builder = new GsonBuilder()
    builder.setDateFormat(DATE_FORMAT1)
    builder.registerTypeAdapter(classOf[Date], new JsonDeserializer[Date] {
      override def deserialize(json: JsonElement, t: Type, ctx: JsonDeserializationContext): Date = {
        val jsonPrim = json.getAsJsonPrimitive()
        if (jsonPrim.isNumber())
          new Date(jsonPrim.getAsLong())
        else
          parseDate(jsonPrim.getAsString())
      }
    })
    builder.create()
  }

  /**
   * Attempts to parse the given date string using the (2) known date formats
   * (e.g. "yyyy-MM-dd HH:mm:ss" and "MMM dd',' yyyy hh:mm:ss a")
   */
  private def parseDate(dateString: String): Date = {
    import scala.util.{ Try, Success, Failure }

    // attempt to parse format #1, then #2, then fail...
    Try((new SimpleDateFormat(DATE_FORMAT1)).parse(dateString))
      .getOrElse(Try((new SimpleDateFormat(DATE_FORMAT2)).parse(dateString))
        .getOrElse(throw new IllegalStateException(s"OooiJson: Unable to parse date '$dateString'")))

  }

}
