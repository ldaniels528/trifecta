package com.github.ldaniels528.trifecta.util

import java.util.concurrent.atomic.AtomicBoolean

/**
  * Once is a cleaver abstance for a one-time only initialization of an instance
  * @author lawrence.daniels@gmail.com
  */
class Once[T](initializer: => T) {
  private val initialized = new AtomicBoolean(false)
  private var instance: T = _

  def apply(): T = {
    if(initialized.compareAndSet(false, true)) instance = initializer
    instance
  }
}

/**
  * Once is a cleaver abstance for a one-time only initialization of an instance
  * @author lawrence.daniels@gmail.com
  */
object Once {

  def apply[T](initializer: => T) = new Once(initializer)

}
