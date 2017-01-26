package com.github.ldaniels528.trifecta.sjs

import io.scalajs.dom.html.browser.encodeURI

/**
  * services package object
  * @author lawrence.daniels@gmail.com
  */
package object services {

  /**
    * Short-cut for encoding URL's
    * @param url the given URL
    */
  implicit class EncodeURIExtension(val url: String) extends AnyVal {

    @inline
    def encode: String = encodeURI(url)

  }

}
