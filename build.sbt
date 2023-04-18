name := "verve-group"
version := "0.0.1"
organization := "com.verve.assignment"

scalaVersion := "2.13.10"


Compile / mainClass := Some("com.verve.assignment.Main")
//packageBin / mainClass := Some("com.verve.assignment.Main")
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"


libraryDependencies ++= Seq(
  //CONFIG
  "com.typesafe" % "config" % "1.4.2",
  //LOG
//  "ch.qos.logback" % "logback-classic" % "1.4.6",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  // JSON
  "org.json4s" %% "json4s-native" % "3.6.6",
  //SPARK
  "org.apache.spark" %% "spark-sql" % "3.3.2"
  
)

