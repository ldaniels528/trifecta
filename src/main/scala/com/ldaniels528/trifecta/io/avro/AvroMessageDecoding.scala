package com.ldaniels528.trifecta.io.avro

import com.ldaniels528.trifecta.messages.MessageDecoder
import org.apache.avro.generic.GenericRecord

/**
 * Avro Message Decoding Capability
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait AvroMessageDecoding extends MessageDecoder[GenericRecord]
