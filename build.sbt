import sbt._

/*
 * Dependencies
 */
val unbescaped = "org.unbescape" % "unbescape" % "1.1.3.RELEASE"
//val xml = "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
val spark = "org.apache.spark" %% "spark-core" % "1.5.1"

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
scalaVersion := "2.10.4"
//libraryDependencies ++= testDependencies
libraryDependencies += unbescaped
//libraryDependencies += xml
libraryDependencies += spark