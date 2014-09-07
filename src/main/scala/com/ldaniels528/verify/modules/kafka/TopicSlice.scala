package com.ldaniels528.verify.modules.kafka

/**
 * Type-safe Topic/Paritition definition
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
case class TopicSlice(name: String, partition: Int)