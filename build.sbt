name := "PremierLeagueTraining"

version := "0.1"

scalaVersion := "2.12.7"

val SparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion
)

