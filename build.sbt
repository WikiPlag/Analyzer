import sbt._


resolvers += "hadoop" at "https://repository.cloudera.com/artifactory/public/"
resolvers += "jitpack" at "https://jitpack.io"

/*
 * Dependencies
 */
val spark = "org.apache.spark" %% "spark-core" % "1.5.1" % "compile"
val hdfs = "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.6.1"
val utils = "com.github.WikiPlag" % "wikiplag_utils" % "-SNAPSHOT"

/*
 * Test-Dependencies
 */
val testDependencies = Seq(
  "org.slf4j" % "slf4j-simple" % "1.7.21" % "test",
  "junit" % "junit" % "4.11" % "test",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

/*	
 * Settings
 */
organization := "HTW Berlin"
name := "WikiPlagAnalyzer"
version := "0.0.1"
scalaVersion := "2.10.6"
libraryDependencies ++= testDependencies
libraryDependencies += spark
libraryDependencies += hdfs
libraryDependencies += utils
