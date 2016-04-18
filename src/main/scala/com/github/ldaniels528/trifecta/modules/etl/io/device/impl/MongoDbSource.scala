package com.github.ldaniels528.trifecta.modules.etl.io.device.impl

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import play.api.libs.json.{JsString, _}

/**
  * MongoDB Source
  * @author lawrence.daniels@gmail.com
  */
trait MongoDbSource {

}

/**
  * MongoDB Source Companion Object
  * @author lawrence.daniels@gmail.com
  */
object MongoDbSource {

  // register the time/date helpers
  RegisterJodaTimeConversionHelpers()

  /**
    * Creates a collection of server address instances from the given hosts string
    * @param hosts given hosts string (e.g. "server1:27017,server2:27017,server3:27018")
    * @return a collection of [[ServerAddress server address]] instances
    */
  def makeServerList(hosts: String): List[ServerAddress] = {
    hosts.split("[,]").toList flatMap { pair =>
      pair.split("[:]").toList match {
        case host :: port :: Nil => Option(new ServerAddress(host, port.toInt))
        case host :: Nil => Option(new ServerAddress(host, 27017))
        case _ => None
      }
    }
  }

  def toDocument(js: JsObject) = {
    js.fieldSet.foldLeft(DBObject()) { case (dbo, (name, jv)) =>
      dbo.put(name, unwrap(jv))
      dbo
    }
  }

  def unwrap(jv: JsValue): AnyRef = {
    jv match {
      case ja: JsArray => ja.value.map(unwrap)
      case jb: JsBoolean => jb.value: java.lang.Boolean
      case jn: JsNumber => jn.value.toDouble: java.lang.Double
      case js: JsString => js.value
      case ju =>
        throw new IllegalStateException(s"Unable to unwrap '$ju' (${Option(ju).map(_.getClass.getName).orNull})")
    }
  }

}
