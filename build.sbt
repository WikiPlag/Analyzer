import sbt._

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

/*
 * Dependencies
 */
val unbescaped = "org.unbescape" % "unbescape" % "1.1.3.RELEASE"
val xml = "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
val akka_stream = "com.typesafe.akka" %% "akka-stream" % "2.4.12"

/*
 * Test-Dependencies
 */
val testDependencies = Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" % "scalatest_2.11" % "3.0.0" % "test"
)

/*
 * Settings
 */
organization := "HTW Berlin"
name := "WikiPlagAnalyzer"
version := "0.0.1"
scalaVersion := "2.11.8"
libraryDependencies ++= testDependencies
libraryDependencies += unbescaped
libraryDependencies += xml
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   %  "2.4.12"
)
libraryDependencies += akka_stream
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.7"