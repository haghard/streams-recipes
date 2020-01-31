import SbtPrompt.autoImport._
import scala.sys.process.Process

name := "stream-recipes"

version := "0.1"

scalaVersion := "2.13.1" //"2.12.8"

//for cats on 2.12. No need when on 2.13
//scalacOptions += "-Ypartial-unification"

scalacOptions ++= Seq(
  //"-Xfatal-warnings",
    //"-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Ywarn-unused",
    "-Ypartial-unification"
  ).filter {
    case ("-Yno-adapted-args" | "-Ypartial-unification") if scalaVersion.value.startsWith("2.13") => false
    case _ => true
  }

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
  "Scalaz Bintray Repo" at "https://dl.bintray.com/scalaz/releases",
  "Rossabaker bintray" at "https://dl.bintray.com/rossabaker/maven",
  "Awesome Utilities" at "https://dl.bintray.com/davegurnell/maven",
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "oncue"             at "https://bintray.com/oncue/releases/quiver/",
  Resolver.bintrayRepo("mfglabs", "maven"),
  "indvd00m-github-repo" at "https://raw.githubusercontent.com/indvd00m/maven-repo/master/repository"
  //Resolver.bintrayRepo("codeheroes", "maven")
)

promptTheme := ScalapenosTheme

val akkaStreamV = "2.5.26"
val fs2V = "2.0.1"
val scalazVersion = "7.2.28"

scalafmtOnCompile := true

//val Origami = "1.0-20150902134048-8d00462"

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"        % scalazVersion,
  "org.scalaz"        %% "scalaz-concurrent"  % scalazVersion,
  "org.scalaz"        %% "scalaz-effect"      % scalazVersion,

  "com.chuusai"       %% "shapeless"          % "2.3.3",

  //"org.http4s"        %% "jawn-streamz"   % "0.10.1", //"org.scalaz.stream" %% "scalaz-stream" %  "0.8.6"
  "org.http4s"        %% "jawn-fs2"           % "0.15.0",
  //"org.spire-math"    %% "jawn-spray"         % "0.14.2",

  //"org.http4s"        %% "jawn-fs2"     % "0.9.0",
  //"org.http4s"        %%  "jawn-fs2"      % "0.10.1"

  "co.fs2"            %% "fs2-core"          %  fs2V, //"1.1.0-M1", //"0.10.5",
  "co.fs2"            %% "fs2-io"            %  fs2V,  //"1.1.0-M1", //"0.10.5",
  "co.fs2"            %% "fs2-reactive-streams" % fs2V,

  "org.typelevel"     %% "cats-free"         %  "2.0.0",

  //"com.spinoco"       %% "fs2-zk"            % "0.1.6",
  //"com.spinoco"       %% "fs2-cassandra"     % "0.2.1",
  
  //Users/haghard/.ivy2/local/default/fs2-cache_2.11/1.0/jars/fs2-cache_2.11.jar

  "com.typesafe.akka" %% "akka-stream" % akkaStreamV,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaStreamV,

  "com.typesafe.akka" %% "akka-http" % "10.1.10",

  //"com.typesafe.akka" %% "akka-actor-typed" % akkaStreamV,

  //("org.squbs" %% "squbs-pattern" %  "0.12.0").excludeAll("com.typesafe.akka"),
  "net.openhft" % "chronicle-queue" % "4.16.5",

  //"com.twitter"   %% "algebird-core" % "0.13.5", //prevents me from jumping on 2.13

  "org.hdrhistogram"  %  "HdrHistogram"      % "2.1.10",
  "com.esri.geometry" %  "esri-geometry-api" % "1.2.1",
  "io.spray"          %% "spray-json"        % "1.3.5",

  //https://github.com/mikolak-net/travesty
  //"net.mikolak" %% "travesty" % s"0.9_$akkaStreamV",

  //"org.scalaz"      %%  "scalaz-zio"      % ZIOVersion,

  //elastic load balancer, build only for 2.11
  //"akka-http-lb" %% "akka-http-lb" % "0.5.0",

  /*
  "com.ambiata"       %%  "origami-core"       %   Origami,
  ("com.ambiata"      %%  "origami-stream"     %   Origami)
    .exclude("com.google.caliper","caliper")
    .exclude("com.google.guava", "guava")
    .exclude("org.scalaz", "scalaz-stream"),
  */

  //"io.swave"          %%  "swave-core"      % "0.7.0",

  //"io.verizon.quiver"      %% "core"        % "6.0.0-scalaz-7.2-SNAPSHOT",

  //"com.typesafe.akka" %% "akka-stream-contrib" % "0.9",

  //"co.adhoclabs"    && "akka-http-contrib" % "0.0.6"

  //Future to Task and Task to Future conversions
  //"io.verizon.delorean" %% "core"       % "1.1.37",


  //https://github.com/moia-dev/streamee
  //"io.moia" %% "streamee" % "5.0.0",

  "org.apache.commons" % "commons-collections4" % "4.0",

  // li haoyi ammonite repl embed
  ("com.lihaoyi" % "ammonite" % "1.9.2" % "test").cross(CrossVersion.full)
)

//compile in Compile := (compile in Compile).dependsOn(compileAvro).value

def compileAvro = baseDirectory map { dir =>
  
  def execute: Int = {
    //rm -rf ./src/main/java/recipes/chat/domain && java -jar ./avro/avro-tools-1.9.1.jar compile schema ./src/main/resources/UsersEnvelopeV1.avsc ./src/main/java
    val schema = IO.listFiles(new File(s"$dir/src/main/resources/avro")).head.getAbsolutePath
    Process(s"rm -rf $dir/src/main/java/recipes/chat/domain").!
      Process(s"java -jar ./avro/avro-tools-1.9.1.jar compile schema $schema $dir/src/main/java") .!

    //Process("pwd").! & Process(s"ls -la $dir/src/main/resources/avro").!
  }

  println("compile avro resources ...")
  haltOnCmdResultError(execute)
}

def haltOnCmdResultError(result: Int) {
  if (result != 0) throw new Exception("Build failed")
}

//test:run
sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

ThisBuild / turbo := true

addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.10.3")

//cancelable in Global := true