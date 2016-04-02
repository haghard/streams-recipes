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

scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.defaultLocal,
  Resolver.mavenLocal,
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/",
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
  "bintray/rossabaker" at "http://dl.bintray.com/rossabaker/maven",
  "Awesome Utilities" at "https://dl.bintray.com/davegurnell/maven",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  Resolver.bintrayRepo("mfglabs", "maven")
)

javacOptions += "-Xmx2G"

promptTheme := ScalapenosTheme

val akkaStreamV = "2.4.3"

libraryDependencies ++= Seq(
  "org.http4s"        %% "jawn-streamz"   % "0.8.1", //https://github.com/rossabaker/jawn-streamz
  "org.spire-math"    %% "jawn-spray"     % "0.8.4",
  //"io.underscore"     %% "csvside"        % "0.10.1",
  //"com.chuusai"       %% "shapeless"      % "2.3.0",
  "org.hdrhistogram"  %  "HdrHistogram"   % "2.1.7",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",
  "com.typesafe.akka" %% "akka-stream"       % akkaStreamV,
  "com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",
  "io.reactivex"      %% "rxscala"           % "0.26.0",
  "co.fs2"            %% "fs2-core"          % "0.9.0-SNAPSHOT"
)

/*
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core"  % "0.9.0",
  "com.typesafe.akka" %% "akka-http-experimental"       % akkaStreamV,
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaStreamV,
  "com.typesafe.play" %% "play-json"                        % "2.4.6"
*/

cancelable in Global := true