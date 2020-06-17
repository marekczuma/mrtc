name := "rovers-simulator"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase-common" % "2.0.6",
  "org.apache.hbase" % "hbase-client" % "2.0.6",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)