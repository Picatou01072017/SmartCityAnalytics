// build.sbt — Option B (sbt run)
ThisBuild / organization := "com.SmartCityAnalytics"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0-SNAPSHOT"

name := "SmartCityAnalytics"

Compile / mainClass := Some("com.SmartCityAnalytics.analytics.MainApp")

// Utile avec Java 17 (modules JDK ouverts pour Spark/Netty/JNA)
ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED"
)

// IMPORTANT pour que les javaOptions soient prises en compte avec `run`
Compile / run / fork := true

// Spark 3.5.x (Scala 2.12)
lazy val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  // Spark sur le classpath pour `sbt run` (PAS en provided)
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  "com.typesafe"      % "config"     % "1.4.3"
)

// (Optionnel) un peu de confort compile
scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "utf8")
