import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

assemblySettings

name := "trifecta"

organization := "com.ldaniels528"

version := "0.18.13"

scalaVersion := "2.11.4"

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.6", "-unchecked",
  "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint")

javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars")

mainClass in assembly := Some("com.ldaniels528.trifecta.TrifectaShell")

test in assembly := {}

jarName in assembly := "trifecta.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("stax", "stax-api", xs @ _*) => MergeStrategy.first
    case PathList("log4j-over-slf4j", xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

// General Dependencies
libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.1",
  "com.101tec" % "zkclient" % "0.4",
  "com.twitter" %% "bijection-core" % "0.7.0",
  "com.twitter" %% "bijection-avro" % "0.7.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.7",
  "jline" % "jline" % "2.12",
  "net.liftweb" %% "lift-json" % "3.0-M2",
  "org.apache.avro" % "avro" % "1.7.7",
  "org.apache.curator" % "curator-framework" % "2.7.0",
  "org.apache.curator" % "curator-test" % "2.7.0",
  "org.apache.kafka" %% "kafka" % "0.8.2-beta"
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.slf4j", "log4j-over-slf4j"),
  "org.apache.storm" % "storm-core" % "0.9.2-incubating"
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.slf4j", "log4j-over-slf4j"),
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",
  "org.mashupbots.socko" %% "socko-webserver" % "0.6.0",
  "org.mongodb" %% "casbah-core" % "2.7.4"
    exclude("org.slf4j", "slf4j-jcl"),
  "org.mongodb" %% "casbah-commons" % "2.7.4"
    exclude("org.slf4j", "slf4j-jcl"),
  "org.fusesource.jansi" % "jansi" % "1.11",
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"
)

// Testing Dependencies
libraryDependencies ++= Seq(
  "junit" % "junit" % "4.11" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test"
)

// define the resolvers
resolvers ++= Seq(
  "Clojars" at "http://clojars.org/repo/",
  "Clojars Project" at "http://clojars.org/org.clojars.pepijndevos/jnativehook",
  "Clojure Releases" at "http://build.clojure.org/releases/",
  "GPhat" at "https://raw.github.com/gphat/mvn-repo/master/releases/",
  "Java Net" at "http://download.java.net/maven/2/",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "Sonatype Repository" at "http://oss.sonatype.org/content/repositories/releases/",
  "Typesafe Releases Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/"
)

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)
