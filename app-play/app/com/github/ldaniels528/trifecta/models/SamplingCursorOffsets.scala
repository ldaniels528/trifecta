package com.github.ldaniels528.trifecta.models

/**
  * Sampling Cursor Offsets
  * @author lawrence.daniels@gmail.com
  */
case class SamplingCursorOffsets(partition: Int,
                                 var topicOffset: Option[Long],
                                 var consumerOffset: Option[Long]) {

  override def toString = s"${getClass.getSimpleName}(partition = $partition, topicOffset = ${topicOffset.orNull}, consumerOffset = ${consumerOffset.orNull})"

}
