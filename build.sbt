name := """geo_analysis"""

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)
