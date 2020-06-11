name := "center"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.1.1",
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"

)