package com.github.ldaniels528.trifecta.sjs.controllers

import com.github.ldaniels528.scalascript.core.HttpPromise
import com.github.ldaniels528.scalascript.{CancellablePromise, Scope}

import scala.concurrent.Future
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js

/**
  * Global Loading Trait
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait GlobalLoading extends js.Object {
  self: Scope =>

  var isLoading: js.Function0[Boolean]
  var loadingStart: js.Function0[CancellablePromise]
  var loadingStop: js.Function1[CancellablePromise, Unit]

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
      val promise = scope.loadingStart()
      val task = HttpPromise.promise2future(httpPromise)
      task onComplete { _ =>
        scope.loadingStop(promise)
      }
      task
    }
  }

}
