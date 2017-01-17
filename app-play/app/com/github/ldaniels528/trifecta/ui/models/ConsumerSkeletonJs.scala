package com.github.ldaniels528.trifecta.ui.models

import play.api.libs.json.{Json, Reads, Writes}

/**
  * Consumer Skeleton
  * @author lawrence.daniels@gmail.com
  */
case class ConsumerSkeletonJs(consumerId: String)

/**
  * Consumer Skeleton Singleton
  * @author lawrence.daniels@gmail.com
  */
object ConsumerSkeletonJs {

  implicit val ConsumerSkeletonReads: Reads[ConsumerSkeletonJs] = Json.reads[ConsumerSkeletonJs]

  implicit val ConsumerSkeletonWrites: Writes[ConsumerSkeletonJs] = Json.writes[ConsumerSkeletonJs]

}
