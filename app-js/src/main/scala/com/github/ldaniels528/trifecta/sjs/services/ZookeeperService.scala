package com.github.ldaniels528.trifecta.sjs.services

import com.github.ldaniels528.meansjs.angularjs.Service
import com.github.ldaniels528.meansjs.angularjs.http.Http
import com.github.ldaniels528.meansjs.util.ScalaJsHelper._
import com.github.ldaniels528.trifecta.sjs.models.{ZkData, ZkItem}

import scala.concurrent.ExecutionContext
import scala.scalajs.js

/**
  * Zookeeper Service
  * @author lawrence.daniels@gmail.com
  */
class ZookeeperService($http: Http) extends Service {

  def getZkData(path: String, format: String)(implicit ec: ExecutionContext) = {
    $http.get[ZkData](s"/api/zookeeper/data?path=$path&format=$format") map (_.data)
  }

  def getZkInfo(path: String)(implicit ec: ExecutionContext) = {
    $http.get[ZkItem](s"/api/zookeeper/info?path=$path") map (_.data)
  }

  def getZkPath(path: String)(implicit ec: ExecutionContext) = {
    $http.get[js.Array[ZkItem]](s"/api/zookeeper/path?path=$path") map (_.data)
  }

}
