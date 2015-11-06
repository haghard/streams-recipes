import SbtPrompt.autoImport._

name := "stream-recipes"

version := "0.1"

scalaVersion := "2.11.7"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

javacOptions += "-Xmx1G"

scalariformSettings

//shellPrompt := { state => "[" + System.getProperty("user.name") + "] " }
//useJGit
//enablePlugins(GitVersioning)
//git.useGitDescribe := true

promptTheme := ScalapenosTheme

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0-M1",
  //"com.typesafe.akka" %% "akka-http-spray-json-experimental" % "1.0",
  "org.scalaz.stream" %% "scalaz-stream" % "0.8"
)