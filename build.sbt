import sbt._
import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._

assemblySettings

name := "trifecta"

organization := "com.ldaniels528"

version := "0.18.18"

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
  "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint")

javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars")

mainClass in assembly := Some("com.ldaniels528.trifecta.TrifectaShell")

test in assembly := {}

jarName in assembly := "trifecta_" + version.value + ".bin.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("stax", "stax-api", xs@_*) => MergeStrategy.first
    case PathList("log4j-over-slf4j", xs@_*) => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

// Avro Dependencies
libraryDependencies ++= Seq(
  "com.twitter" %% "bijection-core" % "0.7.2",
  "com.twitter" %% "bijection-avro" % "0.7.2",
  "org.apache.avro" % "avro" % "1.7.7"
)

// Kafka, Storm and Zookeeper Dependencies
libraryDependencies ++= Seq(
  "com.101tec" % "zkclient" % "0.4",
  "org.apache.curator" % "curator-framework" % "2.7.1",
  "org.apache.curator" % "curator-test" % "2.7.1",
  "org.apache.kafka" %% "kafka" % "0.8.2.0"
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.slf4j", "log4j-over-slf4j"),
  "org.apache.zookeeper" % "zookeeper" % "3.4.6",
  "org.apache.storm" % "storm-core" % "0.9.3"
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.slf4j", "log4j-over-slf4j")
)

// SQL/NOSQL Dependencies
libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.5",
  "org.mongodb" %% "casbah-commons" % "2.8.0"
    exclude("org.slf4j", "slf4j-jcl"),
  "org.mongodb" %% "casbah-core" % "2.8.0"
    exclude("org.slf4j", "slf4j-jcl"),
  "joda-time" % "joda-time" % "2.7",
  "org.joda" % "joda-convert" % "1.7"
)

// General Dependencies
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.9",
  "jline" % "jline" % "2.12",
  "net.liftweb" %% "lift-json" % "3.0-M3",
  "org.mashupbots.socko" %% "socko-webserver" % "0.6.0",
  "org.fusesource.jansi" % "jansi" % "1.11",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2"
)

// Testing Dependencies
libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "org.scalatest" %% "scalatest" % "2.2.3" % "test"
)

resolvers += "Clojars Repo" at "http://clojars.org/repo/"

resolvers += "Clojure Releases" at "http://build.clojure.org/releases/"
