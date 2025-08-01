ThisBuild / scalaVersion := "2.11.12"
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
ThisBuild / javaOptions += "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"


Compile / mainClass := Some("com.ecommerce.analytics.MainApp")

Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/java.net=ALL-UNNAMED"
)

lazy val sparkVersion = "2.2.1"

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "com.typesafe" % "config" % "1.4.3"
    )
  )
