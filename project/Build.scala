import sbt.Keys._
import sbt._


object Build extends Build {
  val AkkaVersion = "2.4.2"
  scalaVersion := "2.11.8"


  val root = Project(
    id = "akka-http-test",
    base = file("."),
    settings = Seq(
      scalaVersion := "2.11.8",
      libraryDependencies ++= Seq(
        "com.typesafe.akka"                %% "akka-actor"                    % AkkaVersion,
  "com.typesafe.akka"                %% "akka-http-experimental"                    % AkkaVersion
      )
    )
  )

}
