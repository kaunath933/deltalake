name := "testdeltalake"

version := "1.0.0"

scalaVersion := "2.12.8"

val sparkVersion: String = "2.4.3"

val scalaTestVersion: String = "3.0.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "io.delta" %% "delta-core" % "0.3.0",
  "org.scalatest" %% "scalatest" % scalaTestVersion
)
