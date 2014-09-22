package com.ldaniels528.trifecta.support.zookeeper

import org.apache.zookeeper.WatchedEvent

/**
 * ZooKeeper Call Back
 * @author Lawrence Daniels <lawrence.daniels@gmail.com>
 */
trait ZkProxyCallBack {

  def process(event: WatchedEvent)

}
