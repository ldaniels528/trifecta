package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Consumer Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ConsumerDelta extends js.Object {
  var consumerId: js.UndefOr[String] = js.native
  var topic: js.UndefOr[String] = js.native
  var partition: js.UndefOr[Int] = js.native
  var offset: js.UndefOr[Int] = js.native
  var topicOffset: js.UndefOr[Int] = js.native
  var lastModified: js.UndefOr[js.Date] = js.native
  var messagesLeft: js.UndefOr[Int] = js.native

}
