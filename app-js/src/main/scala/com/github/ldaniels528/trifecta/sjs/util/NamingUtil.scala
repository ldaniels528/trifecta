package com.github.ldaniels528.trifecta.sjs.util

import scala.annotation.tailrec

/**
  * Naming Utility
  * @author lawrence.daniels@gmail.com
  */
object NamingUtil {

  @tailrec
  def getUntitledName[T](entity: T, prefix: String = "Untitled", index: Int = 1)(nameExists: (T, String) => Boolean): String = {
    val name = s"$prefix$index"
    if (nameExists(entity, name)) getUntitledName(entity, prefix, index + 1)(nameExists) else name
  }

}
