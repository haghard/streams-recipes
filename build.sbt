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
  Resolver.bintrayRepo("haghard", "nosql-join-stream"),
  Resolver.sonatypeRepo("public"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  Resolver.jcenterRepo,
  "Local Maven Repository2" at "file:///Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository",
  "Local Maven Repository3" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "Local Ivy2" at "file://Users/haghard/.ivy2/local/",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  Resolver.bintrayRepo("mfglabs", "maven")
)

promptTheme := ScalapenosTheme

val akkaStreamV = "2.5.2"

//initialCommands in (Test, console) := """ammonite.Main().run()"""

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"    % akkaStreamV,

  //"io.dropwizard.metrics"   % "metrics-graphite" %   "3.1.0.",
  //"com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",
  //"co.adhoclabs"    && "akka-http-contrib" % "0.0.6"

  "org.hdrhistogram"  %  "HdrHistogram"      % "2.1.9",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",
  "com.typesafe.akka" %% "akka-stream-contrib" % "0.8",

  "org.apache.commons" % "commons-collections4" % "4.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "0.9",
  "com.lihaoyi"       % "ammonite" % "0.9.0" % "test" cross CrossVersion.full
)

//streams-recipes/test:console

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3")
// if your project uses multiple Scala versions, use this for cross building
//addCompilerPlugin("org.spire-math" % "kind-projector" % "0.9.3" cross CrossVersion.binary)

cancelable in Global := true