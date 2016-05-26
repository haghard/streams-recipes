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
  "Local Maven Repository2" at "file:///Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository",
  "Local Maven Repository3" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/",
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
  "bintray/rossabaker" at "http://dl.bintray.com/rossabaker/maven",
  "Awesome Utilities" at "https://dl.bintray.com/davegurnell/maven",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "oncue"             at "https://bintray.com/oncue/releases/quiver/",
  //Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns),
  Resolver.bintrayRepo("mfglabs", "maven")
)

javacOptions += "-Xmx2G"

promptTheme := ScalapenosTheme

val akkaStreamV = "2.4.6"

val Origami = "1.0-20150902134048-8d00462"

libraryDependencies ++= Seq(
  "org.http4s"        %% "jawn-streamz"   % "0.8.1", //https://github.com/rossabaker/jawn-streamz
  "com.typesafe.akka" %% "akka-stream"       % akkaStreamV,

  //"com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",

  "com.chuusai"       %% "shapeless"         % "2.3.0",

  "io.reactivex"      %% "rxscala"           % "0.26.0",
  "io.monix"          %% "monix"             % "2.0-M2",
  "co.fs2"            %% "fs2-core"          % "0.9.0-SNAPSHOT",

  "org.spire-math"    %% "jawn-spray"     % "0.8.4",
  "org.hdrhistogram"  %  "HdrHistogram"   % "2.1.7",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",

  "com.ambiata"       %%  "origami-core"            %   Origami,
  ("com.ambiata"      %%  "origami-stream"          %   Origami)
    .exclude("com.google.caliper","caliper")
    .exclude("com.google.guava", "guava")
    .exclude("org.scalaz", "scalaz-stream"),

  "oncue.quiver"      %% "core"              % "5.3.57"
)


/*
libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json"                            % "2.4.6",
  "com.typesafe.akka" %% "akka-http-core"                 %     akkaStreamV,
  "com.typesafe.akka" %% "akka-http-experimental"         %     akkaStreamV,
  "io.spray"          %% "spray-json"                     %     "1.2.6"
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core"     % "0.9.0"
)
*/

cancelable in Global := true