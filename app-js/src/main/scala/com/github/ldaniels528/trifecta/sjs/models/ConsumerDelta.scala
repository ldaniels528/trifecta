package com.github.ldaniels528.trifecta.sjs.models

import scala.scalajs.js

/**
  * Consumer Details
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ConsumerDelta extends js.Object {
  var consumerId: String = js.native
  var topic: String = js.native
  var partition: Int = js.native
  var offset: Int = js.native
  var topicOffset: Int = js.native
  var lastModified: js.UndefOr[js.Date] = js.native
  var messagesLeft: Int = js.native

}
