package com.github.ldaniels528.trifecta.io

import java.io.File
import java.net.{URL, URLClassLoader}

import scala.util.Try

/**
  * Jar File Class Loader
  * @author lawrence.daniels@gmail.com
  */
class JarFileClassLoader(directory: File) extends ClassLoader {
  private val urlClassLoader = createUrlClassLoader()

  override def loadClass(name: String): Class[_] = {
    Try(Class.forName(name, true, urlClassLoader)) getOrElse super.loadClass(name)
  }

  private def createUrlClassLoader() = {
    val jarUrls = getJarFiles(directory).map(_.toURI.toURL).toArray[URL]
    new URLClassLoader(jarUrls, getClass.getClassLoader)
  }

  private def getJarFiles(file: File): List[File] = {
    file match {
      case f if f.isDirectory =>
        val files = Option(f.listFiles()).map(_.toList) getOrElse Nil
        files flatMap getJarFiles
      case f if f.getName.toLowerCase.endsWith(".jar") => f :: Nil
      case _ => Nil
    }
  }

}
