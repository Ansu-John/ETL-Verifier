name := "ETLVerification-sbt"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.scalatest" %% "scalatest" % sparkVersion % "test"