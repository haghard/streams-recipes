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
  Resolver.sonatypeRepo("public"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeRepo("releases"),
  Resolver.bintrayRepo("haghard", "nosql-join-stream"),
  Resolver.jcenterRepo,
  "Local Maven Repository2" at "file:///Volumes/Data/dev_build_tools/apache-maven-3.1.1/repository",
  "Local Maven Repository3" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
  "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
  "bintray/rossabaker" at "http://dl.bintray.com/rossabaker/maven",
  "Awesome Utilities" at "https://dl.bintray.com/davegurnell/maven",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "oncue"             at "https://bintray.com/oncue/releases/quiver/",
  //Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns),
  Resolver.bintrayRepo("mfglabs", "maven")
)

promptTheme := ScalapenosTheme

val akkaStreamV = "2.4.17"

val Origami = "1.0-20150902134048-8d00462"

initialCommands in (Test, console) := """ammonite.Main().run()"""

libraryDependencies ++= Seq(
  "org.http4s"        %% "jawn-streamz"   % "0.8.1", //https://github.com/rossabaker/jawn-streamz
  "com.typesafe.akka" %% "akka-stream"    % akkaStreamV,

  //"com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",

  "io.reactivex"      %% "rxscala"           % "0.26.2",
  "io.monix"          %% "monix"             % "2.1.1",

  "co.fs2"            %% "fs2-core"          % "0.9.4",
  "co.fs2"            %% "fs2-io"            % "0.9.4",

  "com.spinoco"       %% "fs2-zk"            % "0.1.1",
  "com.spinoco"       %% "fs2-cassandra"     % "0.1.7",
  //Users/haghard/.ivy2/local/default/fs2-cache_2.11/1.0/jars/fs2-cache_2.11.jar

  "org.spire-math"    %% "jawn-spray"        % "0.9.0",
  "org.hdrhistogram"  %  "HdrHistogram"      % "2.1.9",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",

  "com.ambiata"       %%  "origami-core"            %   Origami,
  ("com.ambiata"      %%  "origami-stream"          %   Origami)
    .exclude("com.google.caliper","caliper")
    .exclude("com.google.guava", "guava")
    .exclude("org.scalaz", "scalaz-stream"),

  "io.swave"          %%  "swave-core"      % "0.5.0",

  "oncue.quiver"      %% "core"             % "5.3.57",

  "com.typesafe.akka" %% "akka-stream-contrib" % "0.6",

  //"co.adhoclabs"    && "akka-http-contrib" % "0.0.6"


  //Future to Task and Task to Future conversions
  //"io.verizon.delorean" %% "core"       % "1.1.37",

  "org.apache.commons" % "commons-collections4" % "4.0",

  "com.lihaoyi"       % "ammonite" % "0.7.8" % "test" cross CrossVersion.full
)

//streams-recipes/test:console

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

cancelable in Global := true