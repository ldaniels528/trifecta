package com.ldaniels528.trifecta.modules.elasticSearch

import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchOutputWriter(client: ElasticSearchDAO, index: String, indexType: String) extends OutputWriter {

  /**
   * Returns the binary encoding
   * @return the binary encoding
   */
  val encoding: String = "UTF8"

  override def write(key: Array[Byte], message: Array[Byte])(implicit ec: ExecutionContext): Future[_] = {
    val id = Option(key) map (new String(_, encoding))
    val data = new String(message, encoding)
    client.createDocument(index, indexType, id, data, refresh = true)
  }

  override def close(): Unit = ()

}
