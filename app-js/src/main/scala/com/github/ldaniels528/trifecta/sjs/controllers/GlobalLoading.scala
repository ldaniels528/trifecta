package com.github.ldaniels528.trifecta.sjs.controllers

import io.scalajs.npm.angularjs.Scope
import io.scalajs.npm.angularjs.http.HttpResponse

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

  var isLoading: js.Function0[Boolean] = js.native
  var loadingStart: js.Function0[CancellablePromise] = js.native
  var loadingStop: js.Function1[CancellablePromise, Unit] = js.native

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

    @inline
    def withGlobalLoading(implicit scope: GlobalLoading): Future[T] = {
      val promise = scope.loadingStart()
      task onComplete { _ =>
        scope.loadingStop(promise)
      }
      task
    }
  }

  /**
    * Automatic global loading feature
    * @param response the given [[HttpResponse HTTP response]]
    * @tparam T the type of promised result
    */
  implicit class LoadingEnrichmentB[T <: js.Any](val response: HttpResponse[T]) extends AnyVal {

    @inline
    def withGlobalLoading(implicit scope: Scope with GlobalLoading): Future[T] = {
      val promise = scope.loadingStart()
      val task = response.toFuture
      task onComplete { _ =>
        scope.loadingStop(promise)
      }
      task
    }
  }

}
