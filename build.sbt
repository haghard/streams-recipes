import SbtPrompt.autoImport._

import scalariform.formatter.preferences._
import ScalariformKeys._

scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)

name := "stream-recipes"

version := "0.1"

scalaVersion := "2.12.2"

resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  Resolver.bintrayRepo("haghard", "nosql-join-stream")
  /*
  Resolver.sonatypeRepo("public"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  Resolver.jcenterRepo,
  //"Local Maven Repository2" at "file:///Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository",
  //"Local Maven Repository3" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  Resolver.bintrayRepo("mfglabs", "maven")
  */
)

promptTheme := ScalapenosTheme

val akkaStreamV = "2.5.1"

initialCommands in (Test, console) := """ammonite.Main().run()"""

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"    % akkaStreamV,

  //"com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",

  "org.hdrhistogram"  %  "HdrHistogram"      % "2.1.9",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",

  //"io.dropwizard.metrics"   % "metrics-core"   %   "3.1.0.",
  //"io.dropwizard.metrics"   % "metrics-graphite" %   "3.1.0.",

  "com.typesafe.akka" %% "akka-stream-contrib" % "0.6-9-gf073c94",

  //"co.adhoclabs"    && "akka-http-contrib" % "0.0.6"

  "org.apache.commons" % "commons-collections4" % "4.0",

  "com.lihaoyi"       % "ammonite" % "0.8.4" % "test" cross CrossVersion.full
)

//streams-recipes/test:console

//addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")
///addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

cancelable in Global := true