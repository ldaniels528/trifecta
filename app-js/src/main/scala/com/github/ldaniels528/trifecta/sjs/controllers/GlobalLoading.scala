package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.meansjs.angularjs.Scope
import com.github.ldaniels528.meansjs.angularjs.http.HttpPromise
import com.github.ldaniels528.meansjs.util.ScalaJsHelper._

import scala.concurrent.Future
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js

/**
  * Global Loading Trait
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GlobalLoading extends js.Object {
  self: Scope =>

  var isLoading: js.Function0[Boolean]
  var loadingStart: js.Function0[js.Promise[js.Any]]
  var loadingStop: js.Function1[js.Promise[js.Any], Unit]

}

/**
  * Global Loading Companion Object
  * @author lawrence.daniels@gmail.com
  */
object GlobalLoading {

  /**
    * Automatic global loading feature
    * @param task the given [[Future promised task]]
    * @tparam T the type of promised result
    */
  implicit class LoadingEnrichmentA[T](val task: Future[T]) extends AnyVal {

    def withGlobalLoading(implicit scope: GlobalLoading) = {
      val promise = scope.loadingStart()
      task onComplete { _ =>
        scope.loadingStop(promise)
      }
      task
    }
  }

  /**
    * Automatic global loading feature
    * @param httpPromise the given [[HttpPromise HTTP Promise]]
    * @tparam T the type of promised result
    */
  implicit class LoadingEnrichmentB[T <: js.Any](val httpPromise: HttpPromise[T]) extends AnyVal {

    def withGlobalLoading(implicit scope: GlobalLoading) = {
      val task = scope.loadingStart()
      task onComplete { _ =>
        scope.loadingStop(task)
      }
      task
    }
  }

}
