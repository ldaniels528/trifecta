import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

assemblySettings

name := "verify"

version := "0.1"

packageArchetype.java_application

maintainer in Linux := "Lawrence Daniels <lawrence.daniels@gmail.com>"

packageSummary in Linux := "Swiss-Army-Knife for viewing/managing topics for Kafka"

packageDescription := "Swiss-Army-Knife for viewing/managing topics for Kafka"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.6", "-unchecked",
    "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint")

javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.6", "-target", "1.6", "-g:vars")

mainClass in assembly := Some("com.ldaniels528.verify.VerifyShell")

jarName in assembly := "verify.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
  	case PathList("commons-beanutils", xs @ _*) => MergeStrategy.first
    case PathList("org", "fusesource", "jansi", xs @ _*) => MergeStrategy.first
    case PathList("stax", "stax-api", xs @ _*) => MergeStrategy.first
    case PathList("log4j-over-slf4j", xs @ _*) => MergeStrategy.discard
    case PathList("log4j-over-slf4j", xs @ _*) => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}

// General Dependencies
libraryDependencies ++= Seq(
	"com.google.code.gson" % "gson" % "2.2.4",
	"com.twitter" %% "bijection-core" % "0.6.2",
  	"com.twitter" %% "bijection-avro" % "0.6.2",	
//	"com.typesafe.akka" % "akka-actor_2.10" % "2.3.2",
	"commons-beanutils" % "commons-beanutils" % "1.9.1",
	"commons-httpclient" % "commons-httpclient" % "3.1",	
	"commons-io" % "commons-io" % "2.1",
//	"joda-time" % "joda-time" % "2.3",
//	"org.joda" % "joda-convert" % "1.6",
	"log4j" % "log4j" % "1.2.17",
	"org.apache.avro" % "avro" % "1.7.6",	
	"org.apache.kafka" % "kafka_2.10" % "0.8.1.1",
	"org.apache.storm" % "storm-core" % "0.9.2-incubating" % "provided"
)           
            
// Testing Dependencies
libraryDependencies ++= Seq(
	"junit" % "junit" % "4.11" % "test"
)

resolvers ++= Seq(
    "Clojars" at "http://clojars.org/repo/",
	"Clojure-Releases" at "http://build.clojure.org/releases/",
 	"Java Net" at "http://download.java.net/maven/2/",
	"Maven Central Server" at "http://repo1.maven.org/maven2",
	"Sonatype Repository" at "http://oss.sonatype.org/content/repositories/releases/",
	"Typesafe Releases Repository" at "http://repo.typesafe.com/typesafe/releases/",
	"Typesafe Snapshots Repository" at "http://repo.typesafe.com/typesafe/snapshots/"
)
                  
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)