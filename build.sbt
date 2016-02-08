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

scalaVersion := "2.11.7"


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
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/"
)

javacOptions += "-Xmx2G"

promptTheme := ScalapenosTheme

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream"    % "2.4.2-RC2",
  //"org.scalaz.stream" %% "scalaz-stream" % "0.8",
  "org.http4s"        %% "jawn-streamz"   % "0.7.0", //https://github.com/rossabaker/jawn-streamz
  "org.spire-math"    %% "jawn-spray"     % "0.8.3", //"scalaz-stream" % "0.8",
  //"io.underscore"     %% "csvside"        % "0.10.1",
  "org.spire-math"    %% "cats-core"      % "0.4.0-SNAPSHOT",
  "com.chuusai"       %% "shapeless"      % "2.2.5",
  "org.hdrhistogram"  %  "HdrHistogram"   % "2.1.7"
)

cancelable in Global := true