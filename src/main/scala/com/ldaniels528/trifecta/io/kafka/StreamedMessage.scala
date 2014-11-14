package com.ldaniels528.trifecta.io.kafka

import com.ldaniels528.trifecta.messages.BinaryMessage

/**
 * Represents a stream message
 */
case class StreamedMessage(topic: String, partition: Int, offset: Long, key: Array[Byte], message: Array[Byte])
  extends BinaryMessage