import SbtPrompt.autoImport._

import scalariform.formatter.preferences._

scalariformPreferences := scalariformPreferences.value
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    //.setPreference(DoubleIndentConstructorArguments, true)
    //.setPreference(DanglingCloseParenthesis, Preserve)

name := "stream-recipes"

version := "0.1"

//scalaVersion := "2.11.8"
scalaVersion := "2.12.4"

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
  Resolver.bintrayRepo("mfglabs", "maven"),
  "indvd00m-github-repo" at "https://raw.githubusercontent.com/indvd00m/maven-repo/master/repository"
)

promptTheme := ScalapenosTheme

val akkaStreamV = "2.5.11"
val scalazVersion = "7.2.20"

//val Origami = "1.0-20150902134048-8d00462"

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"        % scalazVersion,
  "org.scalaz"        %% "scalaz-concurrent"  % scalazVersion,
  "org.scalaz"        %% "scalaz-effect"      % scalazVersion,

  "com.chuusai"       %% "shapeless"          % "2.3.2",

  "org.http4s"        %% "jawn-streamz"   % "0.10.1", //"org.scalaz.stream" %% "scalaz-stream" %  "0.8.6"
  "org.spire-math"    %% "jawn-spray"     % "0.11.0",

  //"org.http4s"        %% "jawn-fs2"     % "0.9.0",
  //"org.http4s"        %%  "jawn-fs2"      % "0.10.1"

  "com.typesafe.akka" %% "akka-stream"    % akkaStreamV,

  //"com.mfglabs"       %% "akka-stream-extensions-shapeless" % "0.10.0",

  //"io.monix"          %% "monix"             % "2.1.2",

  "co.fs2"            %% "fs2-core"          % "0.10.3",
  "co.fs2"            %% "fs2-io"            % "0.10.3",

  "com.spinoco"       %% "fs2-zk"            % "0.1.6",
  //"com.spinoco"       %% "fs2-cassandra"     % "0.2.1",
  
  //Users/haghard/.ivy2/local/default/fs2-cache_2.11/1.0/jars/fs2-cache_2.11.jar


  "org.hdrhistogram"  %  "HdrHistogram"      % "2.1.9",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.2",

  //https://github.com/mikolak-net/travesty
  "net.mikolak" %% "travesty" % s"0.9_$akkaStreamV",

  //"io.dropwizard.metrics"   % "metrics-core"   %   "3.1.0.",
  //"io.dropwizard.metrics"   % "metrics-graphite" %   "3.1.0.",

  /*
  "com.ambiata"       %%  "origami-core"       %   Origami,
  ("com.ambiata"      %%  "origami-stream"     %   Origami)
    .exclude("com.google.caliper","caliper")
    .exclude("com.google.guava", "guava")
    .exclude("org.scalaz", "scalaz-stream"),
  */

  //"io.swave"          %%  "swave-core"      % "0.7.0",

  //"io.verizon.quiver"      %% "core"        % "6.0.0-scalaz-7.2-SNAPSHOT",

  //"com.typesafe.akka" %% "akka-stream-contrib" % "0.6",

  //"co.adhoclabs"    && "akka-http-contrib" % "0.0.6"


  //Future to Task and Task to Future conversions
  //"io.verizon.delorean" %% "core"       % "1.1.37",

  "org.apache.commons" % "commons-collections4" % "4.0"

  //"com.lihaoyi" % "ammonite" % "1.0.5" % "test" cross CrossVersion.full
)

//initialCommands in (Test, console) := """ammonite.Main().run()"""
/*
sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
  Seq(file)
}.taskValue

(fullClasspath in Test) ++= {
  (updateClassifiers in Test).value
    .configurations
    .find(_.configuration == Test.name)
    .get
    .modules
    .flatMap(_.artifacts)
    .collect{case (a, f) if a.classifier == Some("sources") => f}
}*/

//streams-recipes/test:console

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.4")
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

cancelable in Global := true