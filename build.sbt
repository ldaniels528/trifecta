import sbt.Keys._
import sbt._

val appVersion = "0.22.0rc2"
val meanjsVersion = "0.2.3.1"

val _scalaVersion = "2.11.8"
val akkaVersion = "2.3.14"
val apacheCurator = "3.1.0"
val casbahVersion = "3.1.1"
val kafkaVersion = "0.8.2.0"
val paradiseVersion = "2.1.0"
val playVersion = "2.4.6"
val twitterBijection = "0.9.2"

lazy val scalajsOutputDir = Def.settingKey[File]("Directory for Javascript files output by ScalaJS")

lazy val libDependencies = Seq(
  "log4j" % "log4j" % "1.2.17" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.7",
  "org.slf4j" % "slf4j-log4j12" % "1.7.7" % "test"
)

lazy val testDependencies = Seq(
  "junit" % "junit" % "4.12" % "test",
  "org.mockito" % "mockito-all" % "1.10.19" % "test",
  "org.scalatest" %% "scalatest" % "2.2.3" % "test"
)

lazy val tabular = (project in file("libs/tabular"))
  .settings(
    name := "tabular",
    organization := "com.github.ldaniels528",
    version := "0.1.3",
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars"),
    libraryDependencies ++= libDependencies ++ testDependencies
  )

lazy val commons_helpers = (project in file("libs/commons-helpers"))
  .settings(
    name := "commons-helpers",
    organization := "com.github.ldaniels528",
    version := "0.1.2",
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars"),
    libraryDependencies ++= libDependencies ++ testDependencies
  )

lazy val trifecta_common = (project in file("app-common"))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    name := "trifecta-common",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings")
  )

lazy val trifecta_core = (project in file("."))
  .dependsOn(tabular, commons_helpers)
  .settings(
    name := "trifecta-core",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars"),
    libraryDependencies ++= testDependencies ++ Seq(
      //
      // General Scala Dependencies
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.2", // 0.11.3
      //
      // General Java Dependencies
      "commons-io" % "commons-io" % "2.4",
      "joda-time" % "joda-time" % "2.9.1",
      "net.liftweb" %% "lift-json" % "3.0-M7",
      "org.joda" % "joda-convert" % "1.8.1",
      "org.slf4j" % "slf4j-api" % "1.7.21",
      //
      // Typesafe dependencies
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.play" %% "play-json" % playVersion,
      //
      // Avro Dependencies
      "com.twitter" %% "bijection-core" % twitterBijection,
      "com.twitter" %% "bijection-avro" % twitterBijection,
      "org.apache.avro" % "avro-compiler" % "1.8.1",
      //
      // Kafka and Zookeeper Dependencies
      "org.apache.curator" % "curator-framework" % apacheCurator exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.curator" % "curator-test" % apacheCurator exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.kafka" %% "kafka" % kafkaVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.kafka" % "kafka-clients" % kafkaVersion
    ))

lazy val trifecta_cli = (project in file("app-cli"))
  .dependsOn(commons_helpers, tabular, trifecta_core, trifecta_common)
  .settings(
    name := "trifecta_cli",
    organization := "com.github.ldaniels528",
    version := appVersion + "-" + kafkaVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars"),
    mainClass in assembly := Some("com.github.ldaniels528.trifecta.TrifectaShell"),
    test in assembly := {},
    assemblyJarName in assembly := "trifecta_cli-" + version.value + ".bin.jar",
    assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) => {
      case PathList("log4j-over-slf4j", xs@_*) => MergeStrategy.discard
      case PathList("log4j.properties", xs@_*) => MergeStrategy.discard
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
    },
    libraryDependencies ++= testDependencies ++ Seq(
      //
      // General Scala Dependencies
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.2", // 0.11.3
      //
      // Microsoft/Azure Dependencies
      "com.microsoft.azure" % "azure-documentdb" % "1.5.1",
      "com.microsoft.azure" % "azure-storage" % "4.0.0",
      //"com.microsoft.sqlserver" % "sqljdbc4" % "4.0",
      //
      // SQL/NOSQL Dependencies
      "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
      "org.mongodb" %% "casbah-commons" % casbahVersion exclude("org.slf4j", "slf4j-log4j12"),
      "org.mongodb" %% "casbah-core" % casbahVersion exclude("org.slf4j", "slf4j-log4j12"),
      //
      // General Java Dependencies
      "log4j" % "log4j" % "1.2.17",
      "org.scala-lang" % "jline" % "2.11.0-M3",
      "org.slf4j" % "slf4j-log4j12" % "1.7.21"
    )
  )

lazy val trifecta_ui_angularjs = (project in file("app-js"))
  .dependsOn(trifecta_common)
  .enablePlugins(ScalaJSPlugin)
  .settings(
    name := "trifecta_ui_angularjs",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    relativeSourceMaps := true,
    persistLauncher := true,
    persistLauncher in Test := false,
    resolvers += Resolver.sonatypeRepo("releases"),
    libraryDependencies ++= Seq(
      "com.github.ldaniels528" %%% "scalajs-angularjs-core" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-animate" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-cookies" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-nervgh-fileupload" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-sanitize" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-toaster" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-ui-bootstrap" % meanjsVersion,
      "com.github.ldaniels528" %%% "scalajs-angularjs-ui-router" % meanjsVersion,
      //
      // ScalaJS dependencies
      "com.vmunier" %% "play-scalajs-sourcemaps" % "0.1.0" exclude("com.typesafe.play", "play_2.11"),
      "org.scala-js" %%% "scalajs-dom" % "0.9.0",
      "be.doeraene" %%% "scalajs-jquery" % "0.9.0"
    ))

lazy val trifecta_ui = (project in file("app-play"))
  .aggregate(trifecta_ui_angularjs)
  .dependsOn(commons_helpers, trifecta_core, trifecta_common)
  .enablePlugins(PlayScala, play.twirl.sbt.SbtTwirl, SbtWeb)
  .settings(
    name := "trifecta_ui",
    organization := "com.github.ldaniels528",
    version := appVersion + "-" + kafkaVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.7", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    scalacOptions in(Compile, doc) ++= Seq("-no-link-warnings"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.7", "-target", "1.7", "-g:vars"),
    relativeSourceMaps := true,
    scalajsOutputDir := (crossTarget in Compile).value / "classes" / "public" / "javascripts",
    pipelineStages := Seq(gzip, uglify),
    Seq(packageScalaJSLauncher, fastOptJS, fullOptJS) map { packageJSKey =>
      crossTarget in(trifecta_ui_angularjs, Compile, packageJSKey) := scalajsOutputDir.value
    },
    compile in Compile <<=
      (compile in Compile) dependsOn (fastOptJS in(trifecta_ui_angularjs, Compile)),
    ivyScala := ivyScala.value map (_.copy(overrideScalaVersion = true)),
    libraryDependencies ++= Seq(cache, filters, json, ws,
      //
      // Web Jar dependencies
      //
      "org.webjars" % "angularjs" % "1.4.8",
      "org.webjars" % "angularjs-toaster" % "0.4.8",
      "org.webjars" % "angular-highlightjs" % "0.4.3",
      "org.webjars" % "angular-ui-bootstrap" % "0.14.3",
      "org.webjars" % "angular-ui-router" % "0.2.13",
      "org.webjars" % "bootstrap" % "3.3.6",
      "org.webjars" % "font-awesome" % "4.5.0",
      "org.webjars" % "highlightjs" % "8.7",
      "org.webjars" % "jquery" % "2.1.3",
      "org.webjars" % "nervgh-angular-file-upload" % "2.1.1",
      "org.webjars" %% "webjars-play" % "2.4.0-2"
    ))

// loads the jvm project at sbt startup
onLoad in Global := (Command.process("project trifecta_cli", _: State)) compose (onLoad in Global).value

