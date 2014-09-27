package com.ldaniels528.trifecta.modules.elasticSearch

import com.ldaniels528.trifecta.modules.io.OutputWriter
import com.ldaniels528.trifecta.support.elasticsearch.ElasticSearchDAO
import com.ldaniels528.trifecta.support.kafka.KafkaMicroConsumer.MessageData

import scala.concurrent.{ExecutionContext, Future}

/**
 * Elastic Search Output Writer
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
class ElasticSearchOutputWriter(client: ElasticSearchDAO, index: String, indexType: String) extends OutputWriter {

  override def write(md: MessageData)(implicit ec: ExecutionContext): Future[_] = {
    val id = Option(md.key) map (new String(_, "UTF8"))
    val data = new String(md.message, "UTF8")
    client.createDocument(index, indexType, id, data, refresh = true)
  }

  override def close(): Unit = ()

}
