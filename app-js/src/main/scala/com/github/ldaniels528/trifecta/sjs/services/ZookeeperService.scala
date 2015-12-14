package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.scalascript.Service
import com.github.ldaniels528.scalascript.core.Http
import com.github.ldaniels528.trifecta.sjs.models.{ZkData, ZkItem}

import scala.scalajs.js

/**
  * Zookeeper Service
  * @author lawrence.daniels@gmail.com
  */
class ZookeeperService($http: Http) extends Service {

  def getZkData(path: String, format: String) = {
    $http.get[ZkData](s"/api/zookeeper/data?path=$path&format=$format")
  }

  def getZkInfo(path: String) = {
    $http.get[ZkItem](s"/api/zookeeper/info?path=$path")
  }

  def getZkPath(path: String) = {
    $http.get[js.Array[ZkItem]](s"/api/zookeeper/path?path=$path")
  }

}
