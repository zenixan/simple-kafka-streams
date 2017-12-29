name := "Simple Kafka Streams"
organization := "org.eu.fuzzy.kafka"
description := "A Scala wrapper for the Kafka Streams API."
version := "0.1.0"

//
// License details
//
licenses := Seq(
  ("MIT License", url("https://spdx.org/licenses/MIT.html"))
)

developers := List(
  Developer("zenixan", "Yevhen Vatulin", "zenixan@gmail.com", url("https://fuzzy.eu.org"))
)

//
// Other project settings
//
normalizedName := "simple-kafka-streams"
homepage := Some(url("https://github.com/zenixan/simple-kafka-streams"))
startYear := Some(2017)

//
// Project dependencies
//
val kafkaVersion = "1.0.0"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion classifier "test"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion classifier "test"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test

//
// Build options
//
scalaVersion := "2.12.4"
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Ywarn-dead-code")
