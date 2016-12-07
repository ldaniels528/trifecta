package com.github.ldaniels528.trifecta.messages

import com.github.ldaniels528.commons.helpers.OptionHelper._
import com.github.ldaniels528.commons.helpers.StringHelper._
import com.github.ldaniels528.trifecta.messages.MessageSourceFactory._

import scala.collection.concurrent.TrieMap

/**
  * Message Source Factory
  * @author lawrence.daniels@gmail.com
  */
class MessageSourceFactory() {
  private val readers = TrieMap[String, MessageReader]()
  private val writers = TrieMap[String, MessageWriter]()

  def addReader(prefix: String, reader: MessageReader): this.type = {
    readers += prefix -> reader
    this
  }

  def addWriter(prefix: String, writer: MessageWriter): this.type = {
    writers += prefix -> writer
    this
  }

  def findInputSource(url: String): Option[MessageInputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed input source URL: $url")

    readers.get(prefix).flatMap(_.getInputSource(url))
  }

  def findOutputSource(url: String): Option[MessageOutputSource] = {
    // get just the prefix
    val (prefix, _) = parseSourceURL(url).orDie(s"Malformed output source URL: $url")

    writers.get(prefix).flatMap(_.getOutputSource(url))
  }

}

/**
  * Message Source Factory Companion
  * @author lawrence.daniels@gmail.com
  */
object MessageSourceFactory {

  /**
    * Parses the the prefix and path from the I/O source URL
    * @param url the I/O source URL
    * @return the tuple represents the prefix and path
    */
  def parseSourceURL(url: String): Option[(String, String)] = {
    url.indexOptionOf(":") map url.splitAt map { case (left, right) => (left, right.drop(1)) }
  }

}