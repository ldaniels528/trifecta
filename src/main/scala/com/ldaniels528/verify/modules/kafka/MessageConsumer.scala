package com.ldaniels528.verify.modules.kafka

/**
 * This trait is implemented by classes that are interested in 
 * consuming Kafka messages.
 * @author lawrence.daniels@gmail.com
 */
trait MessageConsumer {

  /**
   * Called when data is ready to be consumed
   * @param offset the message's offset
   * @param message the message as a binary string
   */
  def consume(offset: Long, message: Array[Byte])

}