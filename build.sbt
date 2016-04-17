import sbt.Keys._
import sbt._

val appVersion = "0.20.0"

val _scalaVersion = "2.11.8"
val akkaVersion = "2.3.14"
val apacheCurator = "3.1.0"
val casbahVersion = "3.1.1"
val kafkaversion = "0.9.0.1"
val playVersion = "2.4.6"
val twitterBijection = "0.9.2"

lazy val scalajsOutputDir = Def.settingKey[File]("Directory for Javascript files output by ScalaJS")

lazy val coreDeps = Seq(
  //
  // ldaniels528 Dependencies
  "com.github.ldaniels528" %% "commons-helpers" % "0.1.2",
  "com.github.ldaniels528" %% "tabular" % "0.1.3" exclude("org.slf4j", "slf4j-log4j12"),
  //
  // Akka dependencies
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  //
  // Avro Dependencies
  "com.twitter" %% "bijection-core" % twitterBijection,
  "com.twitter" %% "bijection-avro" % twitterBijection,
  "org.apache.avro" % "avro" % "1.8.0",
  //
  // JSON dependencies
  "com.typesafe.play" %% "play-json" % playVersion,
  //
  // Kafka and Zookeeper Dependencies
  "com.101tec" % "zkclient" % "0.7" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.curator" % "curator-framework" % apacheCurator,
  "org.apache.curator" % "curator-test" % apacheCurator,
  "org.apache.kafka" %% "kafka" % kafkaversion exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.kafka" % "kafka-clients" % kafkaversion,
  "org.apache.zookeeper" % "zookeeper" % "3.4.7" exclude("org.slf4j", "slf4j-log4j12"),
  //
  // Microsoft/Azure Dependencies
  "com.microsoft.azure" % "azure-documentdb" % "1.5.1",
  "com.microsoft.azure" % "azure-storage" % "4.0.0",
  "com.microsoft.sqlserver" % "sqljdbc4" % "4.0",
  //
  // SQL/NOSQL Dependencies
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "org.mongodb" %% "casbah-commons" % casbahVersion exclude("org.slf4j", "slf4j-log4j12"),
  "org.mongodb" %% "casbah-core" % casbahVersion exclude("org.slf4j", "slf4j-log4j12"),
  //
  // General Java Dependencies
  "joda-time" % "joda-time" % "2.9.1",
  "org.joda" % "joda-convert" % "1.8.1"
)

lazy val trifecta_cli = (project in file("app-cli"))
  .settings(
    name := "trifecta_cli",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.8", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.8", "-target", "1.8", "-g:vars"),
    mainClass in assembly := Some("com.github.ldaniels528.trifecta.TrifectaShell"),
    test in assembly := {},
    assemblyJarName in assembly := "trifecta_cli_" + version.value + ".bin.jar",
    assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) => {
      case PathList("stax", "stax-api", xs@_*) => MergeStrategy.first
      case PathList("log4j-over-slf4j", xs@_*) => MergeStrategy.discard
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
    },
    resolvers += "google-sedis-fix" at "http://pk11-scratch.googlecode.com/svn/trunk",
    resolvers += "clojars" at "https://clojars.org/repo",
    resolvers += "conjars" at "http://conjars.org/repo",
    libraryDependencies ++= coreDeps ++ Seq(
      //
      // General Scala Dependencies
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.2", // 0.11.3
      //
      // General Java Dependencies
      "commons-io" % "commons-io" % "2.4",
      "log4j" % "log4j" % "1.2.17",
      "net.liftweb" %% "lift-json" % "3.0-M8",
      "org.fusesource.jansi" % "jansi" % "1.11",
      "org.scala-lang" % "jline" % "2.11.0-M3",
      "org.slf4j" % "slf4j-api" % "1.7.21",
      "org.slf4j" % "slf4j-log4j12" % "1.7.21",
      //
      // Testing dependencies
      "junit" % "junit" % "4.12" % "test",
      "org.mockito" % "mockito-all" % "1.10.19" % "test",
      "org.scalatest" %% "scalatest" % "2.2.3" % "test"
    )
  )

lazy val trifecta_js = (project in file("app-js"))
  .enablePlugins(ScalaJSPlugin)
  .settings(
    name := "trifecta_js",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    relativeSourceMaps := true,
    persistLauncher := true,
    persistLauncher in Test := false,
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.github.ldaniels528" %%% "scalascript" % "0.2.20",
      "com.vmunier" %% "play-scalajs-sourcemaps" % "0.1.0" exclude("com.typesafe.play", "play_2.11"),
      "org.scala-js" %%% "scalajs-dom" % "0.9.0",
      "be.doeraene" %%% "scalajs-jquery" % "0.9.0"
    ))

lazy val trifecta_ui = (project in file("app-play"))
  .aggregate(trifecta_js)
  .dependsOn(trifecta_cli)
  .enablePlugins(PlayScala, play.twirl.sbt.SbtTwirl, SbtWeb)
  .settings(
    name := "trifecta_ui",
    organization := "com.github.ldaniels528",
    version := appVersion,
    scalaVersion := _scalaVersion,
    scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8", "-feature", "-target:jvm-1.8", "-unchecked",
      "-Ywarn-adapted-args", "-Ywarn-value-discard", "-Xlint"),
    javacOptions ++= Seq("-Xlint:deprecation", "-Xlint:unchecked", "-source", "1.8", "-target", "1.8", "-g:vars"),
    relativeSourceMaps := true,
    scalajsOutputDir := (crossTarget in Compile).value / "classes" / "public" / "javascripts",
    pipelineStages := Seq(gzip, uglify),
    Seq(packageScalaJSLauncher, fastOptJS, fullOptJS) map { packageJSKey =>
      crossTarget in(trifecta_js, Compile, packageJSKey) := scalajsOutputDir.value
    },
    compile in Compile <<=
      (compile in Compile) dependsOn (fastOptJS in(trifecta_js, Compile)),
    ivyScala := ivyScala.value map (_.copy(overrideScalaVersion = true)),
    resolvers += "google-sedis-fix" at "http://pk11-scratch.googlecode.com/svn/trunk",
    libraryDependencies ++= coreDeps ++ Seq(cache, filters, json, ws,
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
onLoad in Global := (Command.process("project trifecta_ui", _: State)) compose (onLoad in Global).value

